from datetime import datetime as dt, timedelta
import json
import sys
import time
# to import local fuctions
sys.path.insert(0, '../tools')

import concurrent
from functools import partial
import multiprocessing.dummy as mp
from multiprocessing import Pool, Manager, cpu_count, Manager , Process
from multiprocessing.pool import ThreadPool
import multiprocessing
import os
import os.path
from os import path

import numpy as np
import pandas as pd
from yahooquery import Ticker

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def stocks_pool(stocks_path='../docs/my_stocks.csv'):
    """
    Extract stocks from csv file
    """

    # Read csv with stocks
    my_stocks_df = pd.read_csv(stocks_path, header=0)
    my_stocks = my_stocks_df.values.tolist()

    # Create list with symbols from finviz
    symbols_only = [i[0] for i in my_stocks]

    # Break stocks in chunks so we can run prices and indicators in parallel
    my_stocks_symbols = []
    lenght = int(len(my_stocks) / 16)
    lists = chunks(symbols_only, lenght)
    my_stocks_symbols = [i for i in lists]

    return my_stocks_symbols, my_stocks

def run_prices_list(stocks_list, results_list, start_date, interval):
    """
    Function to run prices on a specific list of stocks and store results in list
    """
    # Create instance with tickers
    tickers = Ticker(stocks_list)

    # Run query
    incremental_prices = tickers.history(start=start_date, interval=interval).reset_index()

    # Append to results
    return results_list.append(incremental_prices)

def prices_current_status():
    """
    Function to import existing price file and extract min and max dates by symbol
    """

    # Import all Prices
    current_prices = pd.read_csv('../docs/prices.csv', header = 0, parse_dates=['timestamp', 'just_date'])

    # Create min and max dates per symbol
    max_date = current_prices.groupby('symbol').max()['just_date'].reset_index()
    max_date.columns = ['symbol', 'just_date_max']
    max_date['just_date_max'] = pd.to_datetime(max_date['just_date_max']) - timedelta(days=2)
    max_date['just_date_max'] = max_date['just_date_max'].dt.strftime('%Y-%m-%d')
    min_date = current_prices.groupby('symbol').min()['just_date'].reset_index()
    min_date.columns = ['symbol', 'just_date_min']

    # Merge both
    min_max_prices = pd.merge(max_date, min_date, on = 'symbol')

    return current_prices, min_max_prices

def last_business_day():
    """
    Function to figure out last business day on a given day to help with the calculation of prices
    """

    # Figure out Last Business Day
    today = dt.today()
    offset = max(1, (today.weekday() + 6) % 7 - 3)
    delta = timedelta(offset)
    most_recent = today - delta
    most_recent_str = most_recent.strftime('%Y-%m-%d')

    return most_recent_str

def interval_start_date(min_max_prices, most_recent_str, full_refresh=False, manual_start='2017-01-01'):
    """
    Calculates interval and start date based on today's date.
    It has the option of running a full refresh, which will update all prices.
    """
    # Chance to run a full_refresh
    if full_refresh == False:

        print('Running Incremental Refresh')

        # Define Interval and start_date
        start_date = min_max_prices['just_date_max'].min()
        today = dt.today()
        today_weekday = today.weekday()

        # Create if statment comparing the min date of prices with most recent business day
        if (most_recent_str == start_date) & (today_weekday not in (5,6)):
            interval = '1h'
        else:
            interval = '1d'

    # If running full_refresh
    else:
        print('Running Full Refresh')
        start_date = manual_start
        interval = '1d'

    return start_date, interval

def run_prices_parallel(my_stocks_symbols, start_date, interval):
    """
    Run price list function in parallel
    """

    # Create Pool and Manager
    pool = mp.Pool(processes=cpu_count() - 1)
    manager = mp.Manager()

    # Create list to Store results
    prices_list = manager.list()

    # Run Pool
    [pool.apply_async(run_prices_list, args=(n, prices_list, start_date, interval)) for n in my_stocks_symbols]
    pool.close()
    pool.join()

    return prices_list

def all_prices_incremental(prices_list, interval, my_stocks, current_prices, start_date):
    """
    Function to incrementally run prices
    """

    # Create dataframe with incremental prices
    incremental_prices = pd.concat(prices_list)

    # Select Only columns that we need
    incremental_prices = incremental_prices[['date', 'high', 'low', 'open', 'close', 'volume', 'symbol']]

    # Create two columns - Interval (price interval) and just date from timestamp
    incremental_prices['interval'] = interval
    incremental_prices['just_date'] = incremental_prices['date'].dt.date

    # Rename columns, so functions don't break
    incremental_prices.columns = ['timestamp', 'high_price', 'low_price', 'open_price', 'close_price', 'volume', 'symbol', 'interval', 'just_date']

    # Merge with more data that we got from finviz (name and industry)
    name_industry = pd.DataFrame(my_stocks, columns=['symbol', 'name', 'industry'])
    incremental_prices = pd.merge(incremental_prices, name_industry, on='symbol')

    # All prices - Concatenate Incremental with Current Prices
    current_prices = current_prices[current_prices['just_date'] < start_date]
    all_prices = pd.concat([incremental_prices, current_prices])
    #all_prices['just_date'] = all_prices['just_date'].astype(str)
    all_prices = all_prices.sort_values(['symbol', 'timestamp']).drop_duplicates(subset=['symbol', 'just_date'], keep='last')

    # Reset Index
    all_prices = all_prices.reset_index(drop=True)
    all_prices.to_csv('../docs/prices.csv', index=0)

    return all_prices

def all_prices_full_refresh(prices_list, interval, my_stocks):
    """
    Function to update all prices (full refresh)
    """
    # Create dataframe with incremental prices
    all_prices = pd.concat(prices_list)

    # Select Only columns that we need
    all_prices = all_prices[['date', 'high', 'low', 'open', 'close', 'volume', 'symbol']]

    # Create two columns - Interval (price interval) and just date from timestamp
    all_prices['interval'] = interval
    all_prices['just_date'] = all_prices['date'].dt.date

    # Rename columns, so functions don't break
    all_prices.columns = ['timestamp', 'high_price', 'low_price', 'open_price', 'close_price', 'volume', 'symbol', 'interval', 'just_date']

    # Merge with more data that we got from finviz (name and industry)
    name_industry = pd.DataFrame(my_stocks, columns=['symbol', 'name', 'industry'])
    all_prices = pd.merge(all_prices, name_industry, on='symbol')

    # All prices - Concatenate Incremental with Current Prices
    #all_prices['just_date'] = all_prices['just_date'].astype(str)

    # Reset Index
    all_prices = all_prices.reset_index(drop=True)
    all_prices.to_csv('../docs/prices.csv', index=0)

    return all_prices

def main_prices(full_refresh=False, do_not_refresh=False, stocks_path='../docs/my_stocks.csv'):

    print('Start Update Prices Process ...')

    # Get list of stocks
    my_stocks_symbols, my_stocks = stocks_pool(stocks_path = stocks_path)

    if do_not_refresh == True:

        print('Using existing prices. No refresh.')

        # Read existing prices and return that
        all_prices = pd.read_csv('../docs/prices.csv')

    else:

        if full_refresh == False:

            # Check if file exists before, otherwise, we need to run a full refresh
            if path.exists('../docs/prices.csv') == False:

                print('Prices File not found. Running a full refresh.')

                # Update variables to run full refresh
                full_refresh = True
                min_max_prices = 0
                most_recent_str = 0

            else:
                # Get min max prices from existing prices
                current_prices, min_max_prices = prices_current_status()

                # Calculate Last Business day
                most_recent_str = last_business_day()

        else:
            min_max_prices = 0
            most_recent_str = 0

        # Calculate start_date and interval
        start_date, interval = interval_start_date(min_max_prices=min_max_prices, most_recent_str=most_recent_str, full_refresh=full_refresh)

        print('Run Prices')
        # Run Prices in Parallel
        prices_list = run_prices_parallel(my_stocks_symbols, start_date, interval)

        if full_refresh == False:
            print('Merging with Existing Prices')

            # Add Incremental to main
            all_prices = all_prices_incremental(prices_list, interval, my_stocks, current_prices, start_date)

        else:
            print('Prices Full Refresh')

            # Full Refresh
            all_prices = all_prices_full_refresh(prices_list, interval, my_stocks)

    # Convert timestamp to datetime
    all_prices['timestamp'] = pd.to_datetime(all_prices['timestamp'])

    return all_prices, my_stocks_symbols
