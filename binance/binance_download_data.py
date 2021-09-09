import asyncio
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


async def download_raw_data(binance_client, interval, coin, quoted, start_time: pd.Timestamp, end_time: pd.Timestamp, verbose=False):
    symbol = coin + quoted
    try:
        raw_df = pd.read_csv(f'cache/{symbol}/{start_time.strftime("%Y_%m_%d")}/{end_time.strftime("%Y_%m_%d")}/{interval}.csv')
        if 'isClose' not in raw_df.columns:
            raw_df['isClose'] = True
        if verbose:
            print(f'read interval {interval} data')
        change_df_types(raw_df)
    except FileNotFoundError:
        if verbose:
            print(f'download interval {interval} data')
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
        raw_df.to_csv(f'cache/{symbol}/{start_time.strftime("%Y_%m_%d")}/{end_time.strftime("%Y_%m_%d")}/{interval}.csv')

    return raw_df


async def __download_data(coin,
                          quoted,
                          start_time,
                          end_time=None,
                          train_size=1.0,
                          verbose=False,
                          train_n_test_intervals=None,
                          train_intervals=None,
                          test_intervals=None):
    """
    Download data from binance and split it into train and test data,
    train meant for the strategy to prepare itself, and test is going to be the simulation itself
    :param symbol: the symbol to download fromm
    :param period: how many days to download
    :param train_size: number in range of [0-1]
    :param verbose: boolean to show what the download data is downloading now
    :param train_n_test_intervals: list of intervals that you want to download for train and test part,
                                    if None download only the 1hour klines
    :param train_intervals: list of intervals that you want to download only for train part
    :param test_intervals: list of intervals that you want to download only for test part
    :return: train dataframe and test dataframe

    todo:: implement the download only the train part
    """
    train = {}
    test = {}
    binance_client = AsyncClient("", "")
    if train_n_test_intervals is None:
        train_n_test_intervals = []
    if test_intervals is None:
        test_intervals = []
    if train_intervals is None:
        train_intervals = []
    train_n_test_dfs = await asyncio.gather(
        *[download_raw_data(binance_client, interval, coin, quoted,
                            start_time=start_time, end_time=end_time, verbose=verbose)
          for interval in train_n_test_intervals]
    )
    for i in range(len(train_n_test_dfs)):
        interval = train_n_test_intervals[i]
        _df = train_n_test_dfs[i]
        train[interval] = _df.iloc[:int(train_size * len(_df))]
        test[interval] = _df.iloc[int(train_size * len(_df)):]
    if train_size == 1:
        await binance_client.session.close()
        return train
    # find the minimum of all tables for starting point
    start_test_date = min([min(ts_entry.index) for ts_entry in test.values() if len(ts_entry.index) > 0])

    tests_df = await asyncio.gather(
        *[download_raw_data(binance_client,
                            interval,
                            coin, quoted,
                            verbose=verbose,
                            start_time=pd.to_datetime(start_test_date).value / 1e6)
          for interval in test_intervals]
    )
    for i in range(len(tests_df)):
        interval = test_intervals[i]
        test[interval] = tests_df[i]
        test[interval]['isClose'] = True

    await binance_client.session.close()
    return train, test


def download_data(*args, **kwargs):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        __download_data(*args, **kwargs))

