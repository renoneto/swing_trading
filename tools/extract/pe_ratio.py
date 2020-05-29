from requests import Session
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '\
                         'AppleWebKit/537.36 (KHTML, like Gecko) '\
                         'Chrome/75.0.3770.80 Safari/537.36'}

def extract_pe():
    """
    Function to extract EPS from websit
    """

    # Create Empty list
    list_to_append = []

    # Read list of stocks and get all symbols
    stocks = pd.read_csv('../docs/my_stocks.csv')
    list_of_stocks = stocks['symbol']

    # Start loop by creating empty list and calculate lenght, so we can track completion
    lenght = len(list_of_stocks)

    # Create Session
    s = Session()

    # Add headers
    s.headers.update(HEADERS)

    # For every single stock, do the following
    for idx, stock in enumerate(list_of_stocks):

        # Print Progress
        print((idx+1)/lenght)

        # Create URL
        url = f'https://widget3.zacks.com/data/chart/json/{stock}/pe_ratio/www.zacks.com'

        # Request and transform response in json
        screener = s.get(url)
        json = screener.json()

        # Check for error
        if len(json) > 1:

            try:
                # Append results into list
                [list_to_append.append([i[0], i[1], stock]) for i in json['weekly_pe_ratio'].items()]
            except KeyError:
                continue

    # Create dataframe with results
    pe_ratio_df = pd.DataFrame(list_to_append)
    pe_ratio_df.columns = ['timestamp', 'pe_ratio', 'symbol']

    # Export
    pe_ratio_df['timestamp'] = pd.to_datetime(pe_ratio_df['timestamp'])
    pe_ratio_df.to_csv('../docs/pe_ratio.csv', index=0)

    return pe_ratio_df

def merge_eps(df, all_prices):
    """
    Function to merge EPS with prices, calculate TTM EPS and PE Ratio
    """

    # Rename columns, convert column to datetime and keep only records where date > 2017-01-01
    df.columns = ['timestamp_merge', 'pe_ratio', 'symbol']
    df['timestamp_merge'] = pd.to_datetime(df['timestamp_merge'])
    df = df[df['timestamp_merge'] > '2017-01-01']

    # Convert all prices column to datetime
    all_prices['just_date_merge'] = pd.to_datetime(all_prices['just_date'])

    # Merge both dataframes
    prices_pe = pd.merge(all_prices,
                    df,
                    left_on = ['just_date_merge', 'symbol'],
                    right_on = ['timestamp_merge', 'symbol'],
                    how='left')

    # Calculate EPS TTM based on weekly PE Ratios
    prices_pe['eps_ttm'] = prices_pe['close_price'] / prices_pe['pe_ratio']
    prices_pe['eps_ttm'] = prices_pe['eps_ttm'].round(3)

    # Since we have only Weekly Value we can Forward/Backward Fill the EPS TTM
    prices_pe['eps_ttm'] = prices_pe.groupby('symbol').ffill()['eps_ttm']
    prices_pe['eps_ttm'] = prices_pe.groupby('symbol').bfill()['eps_ttm']

    # Calculate PE Ratio with EPS TTM and round numbers
    prices_pe['pe_ratio'] = prices_pe['close_price'] / prices_pe['eps_ttm']
    prices_pe['pe_ratio'] = prices_pe['pe_ratio'].round(3)

    # Drop columns
    prices_pe.drop(['just_date_merge', 'timestamp_merge'], inplace=True, axis=1)

    # Export
    prices_pe.to_csv('../docs/pe_prices.csv', index=0)

    return prices_pe

def main_pe(all_prices, full_refresh=False):

    # If a full refresh is not necessary
    if full_refresh == False:

        # Read existing PE Ratios and merge it with all prices
        pe_ratio = pd.read_csv('../docs/pe_ratio.csv')
        pe_ratio = merge_eps(pe_ratio, all_prices)

    else:
        # Extract from website and
        pe_ratio = extract_pe()
        pe_ratio = merge_eps(pe_ratio, all_prices)

    return pe_ratio
