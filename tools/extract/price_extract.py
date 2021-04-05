import sys
# to import local fuctions
sys.path.insert(0, '../tools')

import pandas as pd
from yahooquery import Ticker

def stock_price_min_max_dates(prices_table):
    """
    Function to import existing price file and extract min and max dates by symbol
    """
    # Import all Prices
    current_prices = prices_table.read_table_to_pandas()

    # Create min and max dates per symbol
    min_max_dates = current_prices.groupby('symbol').agg(['min', 'max'])['date'].reset_index()
    min_max_dates.columns = ['symbol', 'min_date', 'max_date']

    # Convert Date columns to string
    min_max_dates[['min_date', 'max_date']] = min_max_dates[['min_date', 'max_date']].astype(str)

    # Change index and convert to List of Dictionaries
    min_max_dates.set_index('symbol', inplace=True)
    min_max_dates = min_max_dates.to_dict('index')

    return min_max_dates

def download_prices(stocks_table, interval='1d', default_min_date='2016-01-01'):
    """
    Function to run prices on a specific list of stocks and store results in list
    """
    # Create list of symbols
    stocks_list = stocks_table.read_table_to_pandas()['symbol'].to_list()

    # Create instance of Tickers
    tickers = Ticker(stocks_list, asynchronous=True, formatted=True, max_workers=32)

    # Run query and create instance with query
    price_query = tickers.history(start=default_min_date, interval=interval)

    # Check result data types
    # If it's a dataframe then all symbols in list are good
    if isinstance(price_query, pd.DataFrame):

        # Reset Index to make the symbol index another column
        stock_prices = price_query.reset_index()

        return stock_prices

def main_prices(stocks_table, prices_table):

    # Get No. of stocks
    n_stocks = len(stocks_table.read_table_to_pandas())

    print(f'Extract prices from {str(n_stocks)} stocks')

    # Run Prices in Parallel
    interval = '1d'
    prices = download_prices(stocks_table, interval)

    # Update Database
    prices_table.load_data(prices, id_columns=['symbol', 'date'], is_replace=True)
