from abc import ABC, abstractmethod

from binance_bot_simulation.exchange_bots.exchange_bot import ExchangeBot


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

    def __init__(self, coins, quoted):
        self.callbacks = Strategy.callbacks
        self.exchange: ExchangeBot = None
        self.coins = coins
        self.quoted = quoted

    def set_exchange(self, exchange):
        self.exchange = exchange

    @property
    def portfolio(self):
        return self.exchange.portfolio

    def prepare_strategy(self):
        pass

    def indicators_graph_objects(self):
        return None

    def candle_close(self, interval, candle):
        callbacks = Strategy.callbacks[self.__module__]
        if interval not in callbacks:
            return
        callback = getattr(self, callbacks[interval])
        callback(interval, candle)
