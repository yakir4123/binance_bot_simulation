import pandas as pd

from exchange_bots.Portfolio import Portfolio
from exchange_bots.ExchangeBot import ExchangeBot, MarketOrder, Order


class SimulationExchangeBot(ExchangeBot):

    def __init__(self, history_data, timestamp, **coins):
        super().__init__(Portfolio(timestamp, 0.1 / 100, **coins), history_data)
        self.__open_orders = []

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

    async def set_order(self, order):
        """
        Add new order to the open orders
        :param order: the order tobe added
        """
        self.open_orders.append(order)
        # order.price = str(order.curr_price)
        # order.cummulativeQuoteQty = str(order.amount)

    def update_orders(self, timestamp: pd.Timestamp, price: float):
        for order in self.open_orders:
            if order.order_type == Order.MARKET:
                self.portfolio.on_order_filled(order, timestamp, price, filled=order.amount)
        # remove all orders that fulfilled
        self.__open_orders = [order for order in self.open_orders if order.total_filled < 1]
