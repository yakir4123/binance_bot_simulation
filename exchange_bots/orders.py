from abc import ABC
from binance import Client


class Order(ABC):
    MARKET = 0
    LIMIT = 1
    STOP_LIMIT = 2
    __id = 0

    def __init__(self, symbol):
        self.id = Order.__id
        self.price = 0
        self.total_filled = 0
        self.symbol = symbol
        Order.__id += 1

    def filled(self, order, timestamp, curr_price, amount_filled):
        pass

    def on_order_canceled(self, order, timestamp):
        pass


class SpotOrder(Order):
    BUY = Client.SIDE_BUY
    SELL = Client.SIDE_SELL

    FEE = 0.1 / 100  # 0.1%

    def __init__(self, order_type, side, coin, quoted, amount, timestamp):
        super().__init__(symbol=coin+quoted)
        self.side = side
        self.coin = coin
        self.quoted = quoted
        self.amount = amount
        self.order_type = order_type
        self.timestamp = timestamp


class MarketSpotOrder(SpotOrder):

    def __init__(self, curr_price, **kwargs):
        super().__init__(order_type=SpotOrder.MARKET, **kwargs)
        self.price = curr_price


class StopLimitSpotOrder(SpotOrder):

    def __init__(self, price, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.price = price


class FutureOrder(Order):
    LONG = 1
    SHORT = -1

    FEE = 0.1 / 100  # 0.1%

    def __init__(self, order_type, position, coin, amount, leverage):
        super().__init__(symbol=coin + 'USDT')
        self.position = position
        self.order_type = order_type
        self.amount = amount
        self.coin = coin
        self.leverage = leverage


class MarketFutureOrder(FutureOrder):

    def __init__(self, position, coin, leverage, usdt_amount, curr_price):
        super().__init__(Order.MARKET, position, coin, usdt_amount / curr_price, leverage)
        self.price = curr_price
