import asyncio
import json
import os.path
import shutil
import sys
import time
from datetime import datetime, date
from typing import Set, List, Dict, Any
from throttler import Throttler
import aiohttp
import pandas as pd
import pandas_market_calendars as mcal
from aiohttp import ClientSession
from tqdm import tqdm

chunk_size = 25_000


def get_market_dates(start: date, end: date) -> List[date]:
    nyse_calendar = mcal.get_calendar('NYSE')
    days = nyse_calendar.valid_days(start_date=start, end_date=end)
    return [datetime.strptime(str(day).split(' ')[0], '%Y-%m-%d').date() for day in days]


async def get_symbols(dates: List[date], polygon_api_key: str) -> Set[str]:
    ret: Set[str] = set()
    async with aiohttp.ClientSession() as session:
        requests = [__fetch_grouped_daily_aggs(session, date, polygon_api_key) for date in dates]
        for request in tqdm(asyncio.as_completed(requests), 'Getting symbols', len(dates)):
            snapshot = (await request)['results']
            for symbol_data in snapshot:
                symbol = symbol_data['T']
                if symbol.isalpha() and symbol.isupper():
                    ret.add(symbol)

    return ret


async def __fetch_grouped_daily_aggs(session: ClientSession, date_: date, polygon_api_key: str) -> Dict[str, Any]:
    """Asynchronously get grouped daily aggs data from Polygon.io."""
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_}?adjusted=false&apiKey={polygon_api_key}'
    async with session.get(url) as response:
        return await response.json()


async def download_symbols_aggs(symbols: List[str], start: date, end: date, res_dir: str, polygon_api_key: str):
    throttler: Throttler = Throttler(15)
    polygon_params: str = f'adjusted=false&sort=asc&limit=50000&apiKey={polygon_api_key}'

    symbols_2_next_url: Dict[str, str] = dict()
    while symbols:

        async with aiohttp.ClientSession() as session:

            # Collect async tasks to download stocks' minute aggs
            requests: List = list()
            for symbol in symbols:
                next_url: str = symbols_2_next_url.get(symbol, f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start}/{end}?{polygon_params}')
                requests.append(__fetch_minute_aggs(session, next_url, throttler))

            for request in tqdm(asyncio.as_completed(requests), 'Getting stocks\' 1m aggs', total=len(symbols)):
                # Get info from result
                response = await request
                symbol = response['ticker']
                n_results = response['resultsCount']

                if not n_results:
                    symbols.remove(symbol)
                    continue

                # Create & clean aggs
                df = pd.DataFrame(response['results'])
                df = df[['t', 'o', 'h', 'l', 'c', 'v']]
                df = df.rename(columns={'t': 'time', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})

                # Create directory for stock's aggregates' files
                dir_path: str = f'{res_dir}/{symbol}_'
                if not os.path.isdir(dir_path):
                    os.mkdir(dir_path)

                for i in range(0, len(df), chunk_size):
                    df_chunk = df.iloc[i:i+chunk_size]

                    df_chunk.to_csv(f'{dir_path}/{df["time"].iloc[0]}.csv', index=False)

                # Update next_url or remove symbol
                if 'next_url' in response:
                    symbols_2_next_url[symbol] = f'{response["next_url"].split("?")[0]}?{polygon_params}'
                else:
                    symbols.remove(symbol)


async def __fetch_minute_aggs(session: ClientSession, url: str, throttler: Throttler, tries: int = 0) -> Dict[str, Any]:
        try:
            async with throttler, session.get(url) as response:
                return await response.json()
        # Failed, try again
        except Exception:
            if tries >= 10:
                print("Failed to fetch minute aggs 10 times. Stopping")
                exit()
            time.sleep(tries**2)
            return await __fetch_minute_aggs(session, url, throttler, tries+1)


if __name__ == '__main__':
    # Args
    args = sys.argv[1:]
    start_date = datetime.strptime(args[0], '%Y-%m-%d').date()
    end_date = datetime.strptime(args[1], '%Y-%m-%d').date()
    res_dir = args[2]
    polygon_api_key = args[3]

    if os.path.isdir(res_dir):
        print(f'Removing files in {res_dir!r}...')
        shutil.rmtree(res_dir, ignore_errors=True)  # Clear result directory
    os.makedirs(res_dir)

    market_dates = get_market_dates(start_date, end_date)

    symbols: List[str] = list(asyncio.run(get_symbols(market_dates, polygon_api_key)))
    asyncio.run(download_symbols_aggs(symbols, start_date, end_date, res_dir, polygon_api_key))