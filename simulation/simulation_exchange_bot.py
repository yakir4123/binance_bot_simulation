import pandas as pd

from binance_bot_simulation.exchange_bots.orders import Order
from binance_bot_simulation.exchange_bots.portfolio import Portfolio
from binance_bot_simulation.exchange_bots.exchange_bot import ExchangeBot


class SimulationExchangeBot(ExchangeBot):

    def __init__(self):
        super().__init__()
        self.__open_orders = []

    def create_portfolio(self, timestamp, **coins):
        # coins_prices = {}
        # for coin, train_df in self.history_data.items():
        #     for df in train_df.values():
        #         try:
        #             coin_price = df.loc[close_time, "Close"]
        #             coins_prices[coin] = coin_price
        #             break
        #         except KeyError:
        #             pass
        # initial_portfolio = initial_portfolio.set_prices(**coins_prices)
        coins_prices = {}
        for coin in coins:
            try:
                coins_prices[coin] = min(
                    [self.ohlc['Close'][coin][interval][-1] for interval in self.ohlc['Close'][coin]])
            except KeyError:
                # the quoted value price
                coins_prices[coin] = 1

        coins = {coin: (amount, coins_prices[coin]) for coin, amount in coins.items()}
        self.portfolio = Portfolio(timestamp, 0.1 / 100, **coins)

    @property
    def open_orders(self):
        """
        :return: all of the open orders that this exchange use.
        """
        return self.__open_orders

    def cancel_all_orders(self, timestamp):
        """
         cancel all open orders
        """
        super().cancel_all_orders(timestamp)
        self.__open_orders.clear()

    async def _set_order(self, order):
        """
        Add new order to the open orders
        :param order: the order tobe added
        """
        self.open_orders.append(order)

    async def _close_future_position(self, timestamp, coin, size, curr_price):
        self.portfolio.close_future_position(timestamp, coin, size, curr_price)

    def update_orders(self, timestamp: pd.Timestamp, price: float):
        for order in self.open_orders:
            if order.order_type == Order.MARKET:
                order.total_filled = 1  # all of the order filled in market trades
                order.price = price
                self.portfolio.on_order_filled(order, timestamp)
        # remove all orders that fulfilled
        self.__open_orders = [order for order in self.open_orders if order.total_filled < 1]
