import asyncio
from typing import List

import numpy as np
import pandas as pd

from common import mkdirs
from binance.client import Client, AsyncClient

INTERVALS = [
    Client.KLINE_INTERVAL_1MINUTE,
    Client.KLINE_INTERVAL_3MINUTE,
    Client.KLINE_INTERVAL_5MINUTE,
    Client.KLINE_INTERVAL_15MINUTE,
    Client.KLINE_INTERVAL_30MINUTE,
    Client.KLINE_INTERVAL_1HOUR,
    Client.KLINE_INTERVAL_2HOUR,
    Client.KLINE_INTERVAL_4HOUR,
    Client.KLINE_INTERVAL_6HOUR,
    Client.KLINE_INTERVAL_8HOUR,
    Client.KLINE_INTERVAL_12HOUR,
    Client.KLINE_INTERVAL_1DAY,
    Client.KLINE_INTERVAL_3DAY,
    Client.KLINE_INTERVAL_1WEEK,
    Client.KLINE_INTERVAL_1MONTH
]


def change_df_types(df):
    """
    Change the type of this fields..
    """
    df['Open'] = df['Open'].astype(np.float64)
    df['High'] = df['High'].astype(np.float64)
    df['Low'] = df['Low'].astype(np.float64)
    df['Close'] = df['Close'].astype(np.float64)
    df['Volume'] = df[f'Volume'].astype(np.float64)
    df['Quote asset volume'] = df['Quote asset volume'].astype(np.float64)
    df['Taker buy base asset volume'] = df['Taker buy base asset volume'].astype(np.float64)
    df['Taker buy quote asset volume'] = df['Taker buy quote asset volume'].astype(np.float64)
    try:
        df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
        df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
        df['Close time'] = df['Close time'] + pd.Timedelta(milliseconds=1)
    except ValueError:
        df['Open time'] = pd.to_datetime(df['Open time'])
        df['Close time'] = pd.to_datetime(df['Close time'])
    df.set_index('Close time', inplace=True)


async def download_raw_data(binance_client, interval, coin, quoted, start_time: pd.Timestamp, end_time: pd.Timestamp,
                            verbose=False):
    symbol = coin + quoted
    try:
        raw_df = pd.read_csv(
            f'cache/{symbol}/{start_time.strftime("%Y_%m_%d")}/{end_time.strftime("%Y_%m_%d")}/{interval}.csv')
        if 'isClose' not in raw_df.columns:
            raw_df['isClose'] = True
        if verbose:
            print(f'read interval {symbol} {interval} data')
        change_df_types(raw_df)
    except FileNotFoundError:
        if verbose:
            print(f'download interval {symbol} {interval} data')
        start_str = f'{start_time.timestamp()}'
        end_str = f'{end_time.timestamp()}'
        data = await binance_client.get_historical_klines(symbol, interval, start_str=start_str, end_str=end_str)
        raw_df = pd.DataFrame(data, columns=['Open time',
                                             'Open',
                                             'High',
                                             'Low',
                                             'Close',
                                             'Volume',
                                             'Close time',
                                             'Quote asset volume',
                                             'Number of trades',
                                             'Taker buy base asset volume',
                                             'Taker buy quote asset volume',
                                             'Ignore'])

        minutes_interval = {
            'm': 1,
            'h': 60,
            'd': 24 * 60,
            'w': 7 * 24 * 60,
            'M': 30 * 24 * 60
        }

        raw_df.loc[:, 'Coin'] = coin
        raw_df.loc[:, 'interval'] = interval
        raw_df.loc[:, 'minutes_interval'] = int(interval[:-1]) * minutes_interval[interval[-1]]
        raw_df = raw_df[:-1]  # last candle is not closed yet
        raw_df = raw_df.drop("Ignore", axis=1)
        change_df_types(raw_df)
        if 'isClose' not in raw_df.columns:
            raw_df['isClose'] = True
        mkdirs(f'cache/{symbol}/{start_time.strftime("%Y_%m_%d")}/{end_time.strftime("%Y_%m_%d")}')
        raw_df.to_csv(
            f'cache/{symbol}/{start_time.strftime("%Y_%m_%d")}/{end_time.strftime("%Y_%m_%d")}/{interval}.csv')

    return raw_df


async def __download_data(coins,
                          quoted,
                          start_time: pd.Timestamp,
                          end_time: pd.Timestamp = None,
                          verbose=False,
                          intervals: List[str] = None):
    """
    Download data from binance by coin name and quoted asset name for example coin='ETH' and quoted='BTC' will download
    the dataframe for ETHBTC symbol
    :param coin: the coin price that you want to download
    :param quoted: the quoted asset that you want to download
    :param verbose: boolean to show what the download data is downloading now
    :param intervals: list of intervals that you want to download
    :return: list of all data frames
    """
    binance_client = AsyncClient("", "")
    if intervals is None:
        raise ValueError('intervals parameter must be an interval value or iterator of intervals, interval value can '
                         'be one of [1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M]')
    if isinstance(intervals, str):
        intervals = [intervals]
    dfs = {coin: {} for coin in coins}

    async def download_task(coin, interval):
        dfs[coin][interval] = await download_raw_data(binance_client, interval, coin, quoted,
                                                      start_time=start_time, end_time=end_time, verbose=verbose)

    await asyncio.gather(
        *[download_task(coin, interval)
          for coin in coins for interval in intervals]
    )
    await binance_client.session.close()
    return dfs


def download_data(*args, **kwargs):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        __download_data(*args, **kwargs))
