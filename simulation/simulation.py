import os
import pickle
import asyncio

import pandas as pd

from binance import Client
from typing import Iterable

from binance_bot_simulation.exchange_bots.strategy import Strategy
from common import mkdirs, timing
from binance_bot_simulation.exchange_bots.portfolio import InitialPortfolio
from binance_bot_simulation.simulation.simulation_exchange_bot import SimulationExchangeBot
from binance_bot_simulation.binance.binance_download_data import download_data, change_df_types
from common.plot import plot_simulation


class Simulation:
    """
    The simulation class responsible obviously on the simulation.
    it has a the simulation loop which is got over all the received data that it simulated and start sending the candles
    in order, one by one using the simulator clock.
    If more than 1 DataFrame has received as argument than the simulation of the graph will be simultaneously call them
    """

    def __init__(self,
                 simulation_start_time: pd.Timestamp = None,
                 verbose=True):
        self.verbose = verbose
        self.simulation_data_feeds = {}
        self.simulation_start_time = simulation_start_time
        self.exchange = SimulationExchangeBot()
        
    def create_portfolio(self, **coins):
        self.exchange.create_portfolio(self.simulation_start_time, **coins)

    @property
    def portfolio(self):
        return self.exchange.portfolio

    def add_data_feed(self, coin: str, interval: str, data_feed: pd.DataFrame):
        if coin not in self.simulation_data_feeds:
            self.simulation_data_feeds[coin] = {}
        if interval in self.simulation_data_feeds[coin]:
            raise ValueError(f'{coin}/{interval} is already in the simulation.')
        self.simulation_data_feeds[coin][interval] = data_feed.loc[data_feed.index >= self.simulation_start_time]
        self.exchange.add_history(coin, interval, data_feed.loc[data_feed.index < self.simulation_start_time])

    def add_strategy(self, strategy: Strategy):
        self.exchange.set_strategy(strategy)

    def start(self):
        """
        start the simulation loop,
        The simulation create a loop with tick on the smallest dataframe interval.
        """

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_start())

    async def async_start(self):
        await self.exchange.start()

        concatenate_df = []
        for coin, dfs in self.simulation_data_feeds.items():
            concatenate_df += list(dfs.values())
        concatenate_df = pd.concat(concatenate_df)
        concatenate_df = concatenate_df.rename_axis('Close time').sort_values(
            by=['Close time', 'minutes_interval', 'Coin'],
            ascending=[True, True, True])

        total_ticks = len(concatenate_df)
        verbose_i = 0
        if self.verbose:
            print_progress_bar(verbose_i, total_ticks,
                               prefix=f'{concatenate_df.index[0]}: {self.exchange.portfolio.portfolio_worth():.2f}',
                               suffix=str(self.exchange.portfolio),
                               length=10)
        for row in concatenate_df.itertuples():
            candle = {
                'Close': row.Close,
                'High': row.High,
                'Low': row.Low,
                'Open': row.Open,
                'Volume': row.Volume,
                'isClose': row.isClose,
                'interval': row.interval,
                'Close time': row.Index,
                'Coin': row.Coin
            }
            await self.exchange.record_candle(row.interval, candle)

            if self.verbose:
                verbose_i += 1
                print_progress_bar(verbose_i, total_ticks,
                                   prefix=f'{candle["Close time"]}: {self.exchange.portfolio.portfolio_worth("BTC"):.2f}',
                                   suffix=str(self.exchange.portfolio),
                                   length=10)
        return self.exchange.strategy.portfolio.spot_order_book, concatenate_df

    def plot(self, coin=None, interval=None,
             spot_orders_plot=False,
             future_orders_plot=False,
             portfolio_division=False,
             strategy_worth_as_coin=False,
             strategy_indicators_plot=True,
             strategy_worth_as_quoted=False,
             ):
        if coin is None:
            coin = self.exchange.strategy.coins[0]
        if interval is None:
            interval = list(self.simulation_data_feeds[coin].keys())[0]
        quoted = self.exchange.strategy.quoted

        spot_order_book = None
        if spot_orders_plot:
            spot_order_book = pd.DataFrame(self.portfolio.spot_order_book)
            spot_order_book = spot_order_book.set_index('Time')

        future_order_book = None
        if future_orders_plot:
            future_order_book = pd.DataFrame(self.portfolio.future_order_book)

        coin_worth = None
        if strategy_worth_as_coin:
            coin_worth = self.portfolio.history_worth(name='Strategy', coin=coin)

        quoted_worth = None
        if strategy_worth_as_quoted:
            quoted_worth = self.portfolio.history_worth(name='Strategy', coin=quoted)

        strategy_indicators = None
        if strategy_indicators_plot:
            strategy_indicators = self.exchange.strategy.indicators_graph_objects()

        df = self.simulation_data_feeds[coin][interval]
        plot_simulation(df=df,
                        order_book=spot_order_book,
                        coin_bot_performance=coin_worth,
                        quoted_bot_performance=quoted_worth,
                        future_order_book=future_order_book,
                        strategy_indicators=strategy_indicators
                        )


@timing
def full_simulation(coins: Iterable,
                    quoted: str,
                    train_size: float,
                    strategy_class,
                    start_time: pd.Timestamp,
                    end_time: pd.Timestamp = None,
                    initial_portfolio=None,
                    simulation_data_df=Client.KLINE_INTERVAL_1DAY,
                    verbose=True,
                    save_pickle=True,
                    **strategy_params):
    if end_time is None:
        end_time = pd.Timestamp.now()
    if initial_portfolio is None:
        initial_portfolio = {coin: 0 for coin in coins}
        initial_portfolio[quoted] = 10000
        initial_portfolio = InitialPortfolio(**initial_portfolio)

    cache_folder_name = f'{coins}_{quoted}_{start_time}_{end_time}_{train_size}_{strategy_class.__name__}'
    cache_folder_name = "".join(x for x in cache_folder_name if x.isalnum())
    cache_file_name = ''
    for param in strategy_params.values():
        cache_file_name = f'{cache_file_name}_{param}'

    cache_file_name = "".join(x for x in cache_file_name if x.isalnum())
    try:
        if save_pickle:
            mkdirs(f'cache/simulation/{cache_folder_name}')
            with open(f'cache/simulation/{cache_folder_name}/{cache_file_name}.pkl', 'rb') as fh:
                return pickle.load(fh)
    except FileNotFoundError:
        pass

    train_dfs = {}
    test_dfs = {}
    close_time = pd.Timestamp(year=2000, month=1, day=1)
    for coin in coins:
        if os.path.exists(f'cache/{coin + quoted}/All_Time'):
            train_data = {}
            test_data = {}
            for interval in strategy_class.get_train_and_test_intervals():
                df = pd.read_csv(f'cache/{coin + quoted}/All_Time/{interval}.csv')
                change_df_types(df)
                df = df.loc[(df.index > start_time) & (df.index < end_time)]
                train_data[interval] = df.iloc[:int(train_size * len(df))]
                test_data[interval] = df.iloc[int(train_size * len(df)):]
        else:
            train_data, test_data = download_data(coin=coin,
                                                  quoted=quoted,
                                                  start_time=start_time,
                                                  end_time=end_time,
                                                  train_size=train_size,
                                                  verbose=verbose,
                                                  train_n_test_intervals=strategy_class.get_train_and_test_intervals(),
                                                  train_intervals=strategy_class.get_train_intervals(),
                                                  test_intervals=strategy_class.get_test_intervals())
        train_dfs[coin] = train_data
        test_dfs[coin] = test_data

        close_time = max(*(df.index[-1] for df in train_dfs[coin].values()), close_time)

    coins_prices = {quoted: 1}
    for coin, train_df in train_dfs.items():
        for df in train_df.values():
            try:
                coin_price = df.loc[close_time, "Close"]
                coins_prices[coin] = coin_price
                break
            except KeyError:
                pass
    initial_portfolio = initial_portfolio.set_prices(**coins_prices)
    exchange = SimulationExchangeBot(train_dfs,
                                     close_time,
                                     **initial_portfolio)

    strategy = strategy_class(exchange=exchange,
                              coins=coins,
                              quoted=quoted,
                              **strategy_params)

    simulation = Simulation(strategy=strategy,
                            simulation_data=test_dfs,
                            verbose=verbose)

    simulation.start()
    order_book = exchange.portfolio.spot_order_book

    df = simulation.simulation_data['BTC'][simulation_data_df]
    portfolio = exchange.portfolio

    result = (order_book, portfolio, df)
    if save_pickle:
        with open(f'cache/simulation/{cache_folder_name}/{cache_file_name}.pkl', 'wb') as fh:
            pickle.dump(result, fh)
    return order_book, portfolio, df, simulation


# Print iterations progress
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()
