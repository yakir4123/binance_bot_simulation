import numpy as np
import pandas as pd

from binance_bot_simulation.exchange_bots.exchange_bot import SpotOrder, FutureOrder


class InitialPortfolio:

    def __init__(self, **coins_amount_dict):
        self.init_portfolio = coins_amount_dict
        self.portfolio = None

    def set_prices(self, **coins_prices):
        self.portfolio = {coin: (amount, coins_prices[coin]) for coin, amount in self.init_portfolio.items()}
        return self.portfolio

    def __str__(self):
        return 'initial_portfolio'


class Portfolio:
    """
    Basic class with the functionality that each portfolio need to have, that any exchange can use
    """

    def __init__(self, timestamp, fee, **coins):
        """
        Portfolio is responsible to save the state of the portfolio in the history
        :param timestamp: when the portfolio initialized
        :param fee: the fee for orders, a number in percents [0 - 100]
        :param coins: key-arg of coins and amount, the key is the coin symbols
                        and the arguments are tuple of (amount, price)
        """
        self.start_worth = 0
        self.__fee = fee / 100
        self.last_update = timestamp
        self.future_positions = []

        self.available_coins = list(coins.keys())
        history_columns_names = list(sum([(f'{coin_name} Amount',
                                           f'{coin_name} A. Price'
                                           f'{coin_name} Price')
                                          for coin_name in self.available_coins], ()))

        self.history_dict = {timestamp: {}}
        self.order_book = []
        for coin_symbol, (amount, price) in coins.items():
            self.history_dict[timestamp][f'{coin_symbol} Amount'] = amount
            self.history_dict[timestamp][f'{coin_symbol} A. Price'] = price
            self.history_dict[timestamp][f'{coin_symbol} Price'] = price
            self.start_worth += amount * price
        self.__history = pd.DataFrame(columns=history_columns_names)

    def on_order_filled(self, order, timestamp):
        if isinstance(order, SpotOrder):
            self.spot_order_update(order, timestamp)
        elif isinstance(order, FutureOrder):
            self.future_order_update(order, timestamp)

    def spot_order_update(self, order, timestamp):
        """
        Update the history of the portfolio
        :param order: the order that updated the
        :param timestamp: when this update happened
        """
        order_as_dict = {
            'Time': timestamp,
            'Coin': order.coin,
            'Quoted Symbol': order.quoted,
            'Amount': order.amount,
            'Price': order.price,
            'Side': 'BUY' if order.side == SpotOrder.BUY else 'SELL',
            'filled': order.total_filled,
            'Percent': 0.5
        }

        last_timestamp = self.history_dict[self.last_update].copy()
        if order.side == SpotOrder.BUY:
            amount = order.amount
            quoted = -order.price * amount
            quoted_amount = last_timestamp[f'{order.quoted} Amount']
            if quoted_amount > 0:
                percent = -quoted / quoted_amount
            else:
                percent = 0
            avg = last_timestamp[f'{order.coin} A. Price'] * last_timestamp[f'{order.coin} Amount']
            avg -= quoted
            avg /= (last_timestamp[f'{order.coin} Amount'] + amount)
        elif order.side == 'SELL':
            # percent of sell order
            coin_amount = last_timestamp[f'{order.coin} Amount']
            if coin_amount > 0:
                percent = order.amount / coin_amount
            else:
                percent = 0
            amount = -order.amount
            quoted = -order.price * amount
            avg = last_timestamp[f'{order.coin} A. Price']
        else:
            raise ValueError(f'There is invalid value in order book ({order.side})')

        order_as_dict['Percent'] = percent
        last_timestamp[f'{order.coin} Amount'] += amount
        last_timestamp[f'{order.quoted} Amount'] += quoted
        last_timestamp[f'{order.coin} A. Price'] = avg

        self.order_book.append(order_as_dict)

        self.history_dict[timestamp] = last_timestamp
        self.last_update = timestamp

    def future_order_update(self, order, timestamp):
        position = FuturePositions(timestamp=timestamp,
                                   contract=order.coin + 'USDT',
                                   size=order.amount,
                                   entry_price=order.price,
                                   position=order.position,
                                   leverage=order.leverage)
        self.future_positions.append(position)

    def update_history(self, timestamp, candle):
        if timestamp not in self.history_dict:
            last_timestamp = self.history_dict[self.last_update].copy()
            if len(self.history_dict) == 1000:
                self.__history = pd.concat([self.__history,
                                            pd.DataFrame.from_dict(self.history_dict, orient='index')])
                self.history_dict = {}  # to save only the current value

            self.history_dict[timestamp] = last_timestamp
        self.history_dict[timestamp][f'{candle["Coin"]} Price'] = candle["Close"]
        self.last_update = timestamp

    def history(self, period=0):
        if period > 0:
            if len(self.history_dict) < period:
                self.__history = pd.concat([self.__history,
                                            pd.DataFrame.from_dict(self.history_dict, orient='index')])
                self.history_dict = {}
            return self.__history.iloc[-period]

        if len(self.history_dict) > 0:
            self.__history = pd.concat([self.__history,
                                        pd.DataFrame.from_dict(self.history_dict, orient='index')])
            self.history_dict = {}
        return self.__history

    def amount_of(self, coin, percent=100, as_coin=None):
        """
        :param coin: the coin symbol to check
        :param percent: number in range of [0-100] to calculate amount with percent
        :param as_coin: if you want the amount of coin compare to other coin price
        :return: how many the portfolio has of this coin
        """
        percent /= 100
        if percent > 1 or percent < 0:
            raise ValueError('amount_of.percent must be value between [0-1]')
        if as_coin is None:
            as_coin = 1
        else:
            as_coin = self.history_dict[self.last_update][f'{as_coin} Price']
        try:
            return self.history_dict[self.last_update][f'{coin} Amount'] * percent / as_coin
        except KeyError:
            return self.__history.loc[self.last_update][f'{coin} Amount'] * percent / as_coin

    def dollar_status(self):
        """
        :return: dictionary with each coin symbol and its dollar value ( in portfolio, nor its price)
        """
        res = {}
        for coin in self.available_coins:
            try:
                coin_price = self.history_dict[self.last_update][f'{coin} Price']
            except KeyError:
                coin_price = self.__history.loc[self.last_update][f'{coin} Price']
            res[coin] = self.amount_of(coin) * coin_price
        return res

    def coins_status(self):
        """
        :return: dictionary with each coin symbol and its amount
        """
        res = {}
        for coin in self.available_coins:
            res[coin] = self.amount_of(coin)
        return res

    def avg_price_of(self, coin):
        """
        :param coin: the coin symbol to check
        :return: an average price of coin
        """
        try:
            return self.history_dict[self.last_update][f'{coin} A. Price']
        except KeyError:
            return self.__history[self.last_update][f'{coin} A. Price']

    def portfolio_worth(self, coin=None):
        """
        :return: how much this portfolio worth
        """
        if coin is None:
            coin_price = 1
        else:
            try:
                coin_price = self.history_dict[self.last_update][f'{coin} Price']
            except KeyError:
                coin_price = self.__history.loc[self.last_update][f'{coin} Price']
        return sum(self.dollar_status().values()) / coin_price

    def history_worth(self, coin, name=''):
        history = self.history()
        worth = pd.Series(data=np.zeros(len(history)), index=history.index)
        for av_coin in self.available_coins:
            worth = worth + (history[f'{av_coin} Amount'] * history[f'{av_coin} Price'])
        worth = worth / history[f'{coin} Price']
        worth = worth.rename(name)
        return worth

    def compare_to_invest(self, name='', **coins_amount):
        assert sum(coins_amount.values()) <= 1
        history = self.history()
        worth = pd.Series(data=np.zeros(len(history)), index=history.index)
        for coin, percentage_amount in coins_amount.items():
            amount = self.start_worth * percentage_amount / history[f'{coin} Price'].iloc[0]
            worth = worth + (amount * history[f'{coin} Price'])
        worth = worth.rename(name)
        return worth

    def __str__(self):
        res = ''
        dollar_status = self.dollar_status()
        for coin in self.available_coins:
            amount = Portfolio.__print_blue(f'{self.amount_of(coin):.2f}')
            price = Portfolio.__print_green(f'{self.avg_price_of(coin):.2f}')
            part_of_wallet = dollar_status[coin] / self.portfolio_worth() * 100
            part_of_wallet = Portfolio.__percent_color(f'{part_of_wallet:.1f}')
            res += f'| {amount} {coin} [{price}] {part_of_wallet} '
        return res

    @staticmethod
    def __print_green(out):
        return '\033[92m' + out + '\033[0m'

    @staticmethod
    def __print_blue(out):
        return '\033[94m' + out + '\033[0m'

    @staticmethod
    def __percent_color(out):
        if float(out) < 33:
            color = '\033[96m'
        elif 33 <= float(out) < 66:
            color = '\033[94m'
        else:
            color = '\033[91m'
        return color + out + '%\033[0m'


class FuturePositions:
    __ID = 0
    SHORT = -1
    LONG = 1

    def __init__(self, timestamp, contract, size, entry_price, position, leverage):
        self.id = FuturePositions.__ID
        FuturePositions.__ID += 1
        self.timestamp = timestamp
        self.contract = contract
        self.entry_price = entry_price
        self.position = position
        self.size = size
        self.leverage = leverage
        self.liquid_price = entry_price - entry_price / leverage

    def pnl(self, mark_price):
        """
        :param mark_price: current price
        :return: the profit / loss of this contract for the current price
        """
        return (mark_price - self.entry_price) * self.size * self.position

    def is_got_liquid(self, candle):
        """
        in case that the contract got close during this candle
        the size of this contract is set to 0 and should be closed.
        :param candle: the last candle to check if this candle got closed.
        :return: true if the contract got close
        """
        price = candle['Low']
        # (price / self.entry_price - 1) * self.position <= -1 / self.leverage
        res = self.pnl(price) <= -self.entry_price * self.size / self.leverage
        if res:
            self.size = 0

        return self.size == 0

    def close_position(self, size, mark_price):
        """
        Close a position by the size of values that you want to close
        for example the position on 1 BTC and you want to close only 1/3 in when the price is reached to 60K
        than size should be 1/3 and price needs to be 60k

        * notice this method is assume that the price is the current price and not set a profit limit order for it
        :param size: the size of the contract you want to close
        :param mark_price: the current price of the contract
        :return: the profit / lose of this close contract.
        """
        if size > self.size:
            raise ValueError('Try to close position more than the position has')
        self.size -= size
        return (mark_price - self.entry_price) * size * self.position

