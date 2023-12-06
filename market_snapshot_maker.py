import os
import sys
from datetime import date, time, datetime
from multiprocessing import Pool
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm


def make_market_snapshots(args: Tuple[date, date, List[str]], res_dir: str):
    start_date = args[0]
    end_date = args[1]
    symbols = args[2]

    snapshots: Dict[str, pd.DataFrame] = dict()

    first_candle = pd.to_datetime('09:30:00').time()
    last_candle = pd.to_datetime('15:47:00').time()

    # Load aggs
    for symbol in tqdm(symbols):
        for aggs_file in os.listdir(f'{res_dir}/{symbol}'):
            file_year = int(aggs_file.split('.')[0])
            if not (start_date.year <= file_year <= end_date.year):
                continue

            aggs = pd.read_feather(f'{res_dir}/{symbol}/{aggs_file}')
            aggs = aggs.set_index(pd.to_datetime(aggs['t'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern'))

            last_day = aggs.index[0].date()
            day_is_in_range = start_date < last_day < end_date

            open = None
            high = 0
            low = sys.maxsize
            close = None
            volume = 0
            for i, (index, row) in enumerate(zip(aggs.index, aggs.to_dict('records'))):
                day = index.date()

                # Check if new day
                last_aggs = i == len(aggs) - 1
                if day != last_day or last_aggs:
                    # Save daily aggs
                    if close:
                        ohclv = pd.Series([open, high, low, close, volume],
                                          index=['open', 'high', 'low', 'close', 'volume'])
                        invalid_ohclv = ohclv.isin([0, np.nan]).any()

                        if not invalid_ohclv:
                            snapshot = snapshots.get(last_day.strftime('%Y-%m-%d'), pd.DataFrame())
                            snapshots[last_day.strftime('%Y-%m-%d')] = pd.concat(
                                [snapshot, pd.DataFrame([ohclv], index=[symbol])])

                    # Reset daily aggs
                    open = None
                    high = 0
                    low = sys.maxsize
                    close = None
                    volume = 0

                    # Check if new day is in range
                    day_is_in_range = start_date <= day <= end_date

                    # Update last day
                    last_day = day

                # Do not collect daily aggs data if day is not in range
                if not day_is_in_range:
                    continue

                volume += row['volume']
                if first_candle <= index.time() < last_candle:
                    if not open:
                        open = row['open']
                    if row['high'] > high:
                        high = row['high']
                    if row['low'] < low:
                        low = row['low']
                    close = row['close']

    return snapshots


def evenly_split_list(lst, x):
    avg = len(lst) // x  # Average number of elements per sublist
    remainder = len(lst) % x  # Remaining elements

    result = []
    idx = 0
    for i in range(x):
        sublist_size = avg + 1 if i < remainder else avg
        result.append(lst[idx:idx + sublist_size])
        idx += sublist_size

    return result


if __name__ == '__main__':
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

    args = sys.argv[1:]
    start_date = datetime.strptime(args[0], '%Y-%m-%d').date()
    end_date = datetime.strptime(args[1], '%Y-%m-%d').date()
    res_dir = args[2]

    symbols = evenly_split_list(os.listdir(res_dir), 6)

    snapshots = dict()
    with Pool() as pool:
        results = pool.imap(make_market_snapshots, [(start_date, end_date, symbols_) for symbols_ in symbols])

        for result in tqdm(results, 'Compiling data', len(symbols[0])):
            for day, df in result.items():
                snapshot = snapshots.get(day, pd.DataFrame())
                snapshots[day] = pd.concat([snapshot, df])

    for day, snapshot in tqdm(snapshots.items(), 'Downloading snapshots', len(snapshots)):
        snapshot['volume'] = snapshot['volume'].astype(int)
        snapshot = snapshot.reset_index().rename(columns={'index': 'symbol'})
        snapshot.to_feather(f'{res_dir}/{day}.feather')