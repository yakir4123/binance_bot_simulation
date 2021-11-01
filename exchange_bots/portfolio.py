import numpy as np
import pandas as pd

from binance_bot_simulation.exchange_bots.future_position import FuturePositions
from binance_bot_simulation.exchange_bots.orders import SpotOrder, FutureOrder


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
        self.future_positions = {}

        self.available_coins = list(coins.keys())
        history_columns_names = list(sum([(f'{coin_name} Amount',
                                           f'{coin_name} A. Price'
                                           f'{coin_name} Price')
                                          for coin_name in self.available_coins], ()))

        self.history_dict = {timestamp: {}}
        self.spot_order_book = []
        self.future_order_book = []
        for coin_symbol, (amount, price) in coins.items():
            self.history_dict[timestamp][f'{coin_symbol} Amount'] = amount
            self.history_dict[timestamp][f'{coin_symbol} A. Price'] = price
            self.history_dict[timestamp][f'{coin_symbol} Price'] = price
            self.start_worth += amount * price
        self.history_dict[timestamp]['Future margin balance'] = 0
        self.history_dict[timestamp]['Future unrealized PNL'] = 0
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
        spot_order_as_dict = {
            'Id': order.id,
            'Time': timestamp,
            'Coin': order.coin,
            'Quoted Symbol': order.quoted,
            'Amount': order.amount,
            'Price': order.price,
            'Side': 'BUY' if order.side == SpotOrder.BUY else 'SELL',
            'filled': order.total_filled,
            'Percent': None
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

        spot_order_as_dict['Percent'] = percent
        last_timestamp[f'{order.coin} Amount'] += amount
        last_timestamp[f'{order.quoted} Amount'] += quoted
        last_timestamp[f'{order.coin} A. Price'] = avg

        self.spot_order_book.append(spot_order_as_dict)

        self.history_dict[timestamp] = last_timestamp
        self.last_update = timestamp

    def future_order_update(self, order, timestamp):

        future_order_as_dict = {
            'Id': order.id,
            'Time': timestamp,
            'Symbol': order.symbol,
            'Amount': order.amount,
            'Price': order.price,
            'Side': 'LONG' if order.position == FutureOrder.LONG else 'SHORT',
            'Filled': order.total_filled,
            'Percent': None
        }
        order_margin = order.price * order.amount
        last_timestamp = self.history_dict[self.last_update].copy()
        percent = order_margin / last_timestamp[f'USDT Amount']
        future_order_as_dict['Percent'] = percent
        self.future_order_book.append(future_order_as_dict)

        # add position to the positions list
        if order.symbol not in self.future_positions:
            position = FuturePositions(timestamp=timestamp,
                                       contract=order.symbol,
                                       size=order.amount,
                                       entry_price=order.price,
                                       position=order.position,
                                       leverage=order.leverage,)
            self.future_positions[order.symbol] = position
            last_timestamp['USDT Amount'] -= order_margin
            last_timestamp['Future margin balance'] += order_margin
        else:
            position = self.future_positions[order.symbol]
            if position.leverage != order.leverage:
                raise ValueError('leverage cant be different between same positions')
            # if make the position bigger
            if order.position == position.position:
                position.entry_price = ((position.entry_price * position.size) + (order.price * order.amount)) / (position.size + order.amount)
                position.size += order.amount
                last_timestamp['USDT Amount'] -= order_margin
                last_timestamp['Future margin balance'] += order_margin
            else:
                # if the order is smaller than the actual position
                if order.amount <= position.size:
                    pnl, margin = position.close_position(order.amount, last_timestamp[f'{order.coin} Price'])
                    # add the pnl and the margin to the wallet
                    last_timestamp['USDT Amount'] += pnl + margin

                    # remove this numbers from the future position
                    last_timestamp['Future unrealized PNL'] -= pnl
                    last_timestamp['Future margin balance'] -= margin
                else:
                    pnl, margin = position.close_position(position.size, last_timestamp[f'{order.coin} Price'])
                    # add the pnl and the margin to the wallet
                    last_timestamp['USDT Amount'] += pnl + margin

                    # remove this numbers from the future position
                    last_timestamp['Future unrealized PNL'] -= pnl
                    last_timestamp['Future margin balance'] -= margin

                    # now create new position instead
                    position = FuturePositions(timestamp=timestamp,
                                               contract=order.symbol,
                                               size=order.amount - position.size,
                                               entry_price=order.price,
                                               position=order.position,
                                               leverage=order.leverage)
                    self.future_positions[order.symbol] = position
                    last_timestamp['USDT Amount'] -= order_margin
                    last_timestamp['Future margin balance'] += order_margin

        self.history_dict[timestamp] = last_timestamp
        self.last_update = timestamp

    def close_future_position(self, timestamp, coin, size, curr_price):
        try:
            position = self.future_positions[coin]
        except KeyError:
            return

        new_update = self.history_dict[timestamp].copy()
        pnl, margin = position.close_future_position(size, curr_price)
        print(f'{timestamp}\npnl: {pnl}')
        new_update['USDT Amount'] += pnl + margin

        # remove this numbers from the future position
        new_update['Future unrealized PNL'] -= pnl
        new_update['Future margin balance'] -= margin
        self.history_dict[timestamp] = new_update
        self.last_update = timestamp

    def check_future_position_liquid(self, candle):
        to_remove = []
        for position in self.future_positions:
            if position.is_got_liquid(candle):
                to_remove.append(position.symbol)

        for symbol in to_remove:
            del self.future_positions[symbol]

    def update_history(self, timestamp, candle):
        if timestamp not in self.history_dict:
            last_timestamp = self.history_dict[self.last_update].copy()
            if len(self.history_dict) == 1000:
                self.__history = pd.concat([self.__history,
                                            pd.DataFrame.from_dict(self.history_dict, orient='index')])
                self.history_dict = {}  # to save only the current value

            self.history_dict[timestamp] = last_timestamp
        self.history_dict[timestamp][f'{candle["Coin"]} Price'] = candle["Close"]
        self.history_dict[timestamp]['Future unrealized PNL'] = self.calculate_unrealized_pnl(candle["Close"])
        self.last_update = timestamp

    def calculate_unrealized_pnl(self, curr_price):
        upnl = 0
        for id, position in self.future_positions.items():
            upnl = position.pnl(curr_price)
        return upnl

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

        if self.last_update in self.history_dict:
            last_update = self.history_dict[self.last_update]
        else:
            last_update = self.__history.loc[self.last_update]
        if coin is None:
            coin_price = 1
        else:
            coin_price = last_update[f'{coin} Price']
        future = last_update[f'Future margin balance']
        future += last_update[f'Future unrealized PNL']
        res = sum(self.dollar_status().values()) + future
        return res / coin_price

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

        margin = Portfolio.__print_blue(f'{self.history_dict[self.last_update]["Future margin balance"]:.1f}')
        upnl = Portfolio.__print_blue(f'{self.history_dict[self.last_update]["Future unrealized PNL"]:.1f}')
        res += f'| Future margin [{margin}] '
        res += f'| Future uPNL [{upnl}] '
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

