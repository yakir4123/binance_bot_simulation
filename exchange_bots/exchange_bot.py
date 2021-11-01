from binance import Client
from abc import ABC, abstractmethod
from binance_bot_simulation.other.circular_queue import CircularQueue


class ExchangeBot(ABC):

    def __init__(self, memory_length=500):
        self.strategy = None
        self.portfolio = None
        # history_data will be available only for prepare stage
        self.history_data = {}
        self.memory_length = memory_length
        self.ohlc = {key: {} for key in ['Open', 'High', 'Low', 'Close']}
        self.tasks = []

    def set_strategy(self, strategy):
        self.strategy = strategy
        self.strategy.set_exchange(self)

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

    async def start(self):
        await self.strategy.prepare_strategy()
        # free a lot of ram
        self.history_data = None

    async def record_candle(self, interval, candle):
        [self.ohlc[key][candle['Coin']][interval].enqueue(candle[key]) for key in self.ohlc]
        self.portfolio.update_history(candle['Close time'], candle)

        self.strategy.candle_close(interval, candle)
        await self.update(candle)

    @property
    @abstractmethod
    def open_orders(self):
        """
        :return: all the open orders
        """
        pass

    @abstractmethod
    def update_orders(self, timestamp, price: float):
        pass

    @abstractmethod
    async def _set_order(self, order):
        pass

    @abstractmethod
    def _close_future_position(self, timestamp, coin, size, curr_price):
        pass

    def cancel_all_orders(self, timestamp):
        """
         cancel all open orders and notify the strategy.
         make sure to call to super to notify the strategy that
        """
        [order.on_order_canceled(order, timestamp) for order in self.open_orders]

    def close_future_position(self, symbol, size=None, percent=None):
        if symbol not in self.portfolio.future_positions:
            return
        if (size is None and percent is None) or (size is not None and percent is not None):
            percent = 1
        position_size = self.portfolio.future_positions[symbol] * percent
        if percent is not None:
            size = position_size * percent
        self.tasks.append(ExchangeTask(ExchangeTask.CLOSE_FUTURE_POSITION, symbol=symbol, size=size))

    def set_order(self, order):
        """
        Set order in the open order book
        :param order: The order
        """
        if order.price * order.amount < 20:
            return
        self.tasks.append(ExchangeTask(ExchangeTask.ORDER, order=order))

    async def update(self, candle):
        for task in self.tasks:
            if task.type == ExchangeTask.ORDER:
                await self._set_order(task['order'])
            elif task.type == ExchangeTask.CLOSE_FUTURE_POSITION:
                await self._close_future_position(candle['Close time'], task['coin'], task['size'], candle['Close'])
        self.tasks = []
        self.update_orders(candle['Close time'], candle['Close'])

    def open(self, coin, interval, klines):
        return self.ohlc['Open'][coin][interval][:-klines]

    def high(self, coin, interval, klines=1):
        return self.ohlc['High'][coin][interval][:-klines]

    def low(self, coin, interval, klines=1):
        return self.ohlc['Low'][coin][interval][:-klines]

    def close(self, coin, interval, klines=1):
        return self.ohlc['Close'][coin][interval][:-klines]

    def __str__(self):
        return str(self.portfolio)


class ExchangeTask:
    ORDER = 0
    CLOSE_FUTURE_POSITION = 1

    def __init__(self, type, **kwargs):
        self.type = type
        self.values = kwargs

    def __getitem__(self, item):
        return self.values[item]
