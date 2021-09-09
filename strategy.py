from abc import ABC, abstractmethod


class Strategy(ABC):
    callbacks = {}

    @staticmethod
    def on_candle_close(*intervals):
        def wrapped(callback):
            if callback.__module__ not in Strategy.callbacks:
                Strategy.callbacks[callback.__module__] = {}
            for interval in intervals:
                Strategy.callbacks[callback.__module__][interval] = callback.__name__
            return callback

        return wrapped

    def __init__(self, exchange, coins, quoted):
        self.callbacks = Strategy.callbacks
        self.__exchange = exchange
        self.coins = coins
        self.quoted = quoted

    @property
    def exchange(self):
        return self.__exchange

    @property
    def portfolio(self):
        return self.__exchange.portfolio

    def prepare_strategy(self):
        pass

    @abstractmethod
    def update_new_candle(self, candle, time):
        pass

    @staticmethod
    @abstractmethod
    def get_train_intervals():
        pass

    @staticmethod
    @abstractmethod
    def get_train_and_test_intervals():
        pass

    @staticmethod
    @abstractmethod
    def get_test_intervals():
        pass

    async def candle_close(self, interval, timestamp, candle):
        callbacks = Strategy.callbacks[self.__module__]
        if interval not in callbacks:
            return
        callback = getattr(self, callbacks[interval])
        await callback(interval, timestamp, candle)
        self.__exchange.portfolio.update_history(timestamp, candle)