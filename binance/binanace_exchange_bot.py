import asyncio

import pandas as pd
import schedule as schedule

from binance.client import Client
from binance import BinanceSocketManager
from binance_bot_simulation.exchange_bots.exchange_bot import ExchangeBot
from binance_bot_simulation.binance import binance_portfolio


class BinanaceExchangeBot(ExchangeBot):

    @classmethod
    async def create(cls, client, history_data, intervals, **coins_symbols):
        wallet = await asyncio.gather(*[client.get_asset_balance(asset=symbol) for symbol in coins_symbols.keys()])
        coins = {coin['asset']: (float(coin['free']), coins_symbols[coin['asset']]) for coin in wallet}
        return BinanaceExchangeBot(client, history_data, intervals, coins)

    def __init__(self, client, history_data, intervals, coins):
        self.client = client
        self.klines_df = {interval: {} for interval in intervals}
        timestamp = pd.Timestamp.now()
        schedule.every().day.at("00:04:00").do(self.update_klines)
        super().__init__(binance_portfolio(client, timestamp, **coins), history_data)

    async def kline_listener(self, client, strategy, interval):
        bm = BinanceSocketManager(client)
        print(f'Start listen to {interval}')
        async with bm.kline_socket(symbol=strategy.coin, interval=interval) as stream:
            while True:
                res = await stream.recv()
                kline = res['k']
                candle = {'Open': float(kline['o']),
                          'High': float(kline['h']),
                          'Low': float(kline['l']),
                          'Close': float(kline['c']),
                          'isClose': res['k']['x']}
                timestamp = pd.Timestamp(res['E'] * 1e6)
                self.klines_df[interval][timestamp] = candle
                await strategy.candle_close(interval, timestamp, candle)

    @property
    async def open_orders(self):
        order_book = await self.client.get_open_orders()
        return order_book

    async def cancel_all_orders(self, timestamp):
        pass
        # orders = await self.client.cancel_order(symbol='BTCBUSD', orderId=14086)
        # orders = await self.client.cancel_order(symbol='BTCBUSD', orderId=15178)
        # super.cancel_all_orders(timestamp)
        # return orders

    async def set_order(self, order):
        return await self.client.create_order(symbol=order.coin,
                                              side=order.side,
                                              type=Client.ORDER_TYPE_MARKET,
                                              quantity=order.quantity)

    def data(self, initeval):
        return self.klines_df[initeval]

    def update_klines(self):
        for interval in self.history_data.keys():
            self.history_data[interval] = pd.concat([self.history_data[interval],
                                                     pd.DataFrame.from_dict(self.klines_df[interval], orient='index')])
            self.klines_df[interval] = {}  # to save only the current value
