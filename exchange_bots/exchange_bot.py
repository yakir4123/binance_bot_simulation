from abc import ABC, abstractmethod

from binance import Client

from binance_bot_simulation.other.circular_queue import CircularQueue


class ExchangeBot(ABC):

    def __init__(self, memory_length=500):
        self.portfolio = None
        # history_data will be available only for prepare stage
        self.history_data = {}
        self.number_of_trades = 0
        self.memory_length = memory_length

        self.ohlc = {key: {} for key in ['Open', 'High', 'Low', 'Close']}

    def add_history(self, coin, interval, history_data):
        if coin not in self.history_data:
            self.history_data[coin] = {}
        self.history_data[coin][interval] = history_data

        # { 'Open' :
        #           { coin :
        #                    { interval : []
        #                      ...
        #                    }
        #             ...
        #           }
        #    ...
        # }
        for key in self.ohlc.keys():
            if coin not in self.ohlc[key]:
                self.ohlc[key][coin] = {}
            self.ohlc[key][coin][interval] = CircularQueue(history_data[key].values, self.memory_length)

    def record_candle(self, interval, candle):
        [self.ohlc[key][candle['Coin']][interval].enqueue(candle[key]) for key in self.ohlc]
        self.portfolio.update_history(candle['Close time'], candle)

    @property
    @abstractmethod
    def open_orders(self):
        """
        :return: all the open orders
        """
        pass

    def cancel_all_orders(self, timestamp):
        """
         cancel all open orders and notify the strategy.
         make sure to call to super to notify the strategy that
        """
        [order.on_order_canceled(order, timestamp) for order in self.open_orders]

    @abstractmethod
    async def set_order(self, order):
        """
        Set order in the open order book
        :param order: The order
        """
        pass

    def open(self, coin, interval, klines):
        return self.ohlc['Open'][coin][interval][:klines]

    def high(self, coin, interval, klines=1):
        return self.ohlc['High'][coin][interval][:klines]

    def low(self, coin, interval, klines=1):
        return self.ohlc['Low'][coin][interval][:klines]

    def close(self, coin, interval, klines=1):
        return self.ohlc['Close'][coin][interval][:klines]

    def __str__(self):
        return f'trades = {self.number_of_trades} {str(self.portfolio)}'


class Order(ABC):
    MARKET = 0
    LIMIT = 1
    STOP_LIMIT = 2

    def __init__(self):
        self.price = 0
        self.total_filled = 0

    def filled(self, order, timestamp, curr_price, amount_filled):
        pass

    def on_order_canceled(self, order, timestamp):
        pass


class SpotOrder(Order):
    BUY = Client.SIDE_BUY
    SELL = Client.SIDE_SELL

    FEE = 0.1 / 100  # 0.1%
    __id = 0

    def __init__(self, order_type, side, coin, quoted, amount, timestamp):
        super().__init__()
        self.id = SpotOrder.__id
        self.side = side
        self.coin = coin
        self.quoted = quoted
        self.amount = amount
        self.order_type = order_type
        self.timestamp = timestamp

        SpotOrder.__id += 1


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
    __id = 0

    def __init__(self, order_type, position, coin, amount, leverage):
        super().__init__()
        self.id = FutureOrder.__id
        self.position = position
        self.coin = coin
        self.order_type = order_type
        self.amount = amount
        self.leverage = leverage

        FutureOrder.__id += 1


class MarketFutureOrder(FutureOrder):

    def __init__(self, position, coin, leverage, usdt_amount, curr_price):
        super().__init__(Order.MARKET, position, coin, usdt_amount / curr_price, leverage)
