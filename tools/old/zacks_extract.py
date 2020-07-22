from requests import Session
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '\
                         'AppleWebKit/537.36 (KHTML, like Gecko) '\
                         'Chrome/75.0.3770.80 Safari/537.36'}

def zacks_extract(ratio_name, period='weekly_'):

    """
    Function to extract Ratios from Zacks
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

    # JSON Key Field
    json_field = period + ratio_name

    # For every single stock, do the following
    for idx, stock in enumerate(list_of_stocks):

        # Print Progress
        print((idx+1)/lenght)

        # Create URL
        url = f'https://widget3.zacks.com/data/chart/json/{stock}/' + ratio_name + '/www.zacks.com'

        # Request and transform response in json
        screener = s.get(url)
        json = screener.json()

        # Check for error
        if len(json) > 1:

            try:
                # Append results into list
                [list_to_append.append([i[0], i[1], stock]) for idx, i in enumerate(json[json_field].items()) if idx < 300]

            except (KeyError, AttributeError) as e:
                continue

    # Create dataframe with results
    df = pd.DataFrame(list_to_append)
    df.columns = ['timestamp', ratio_name, 'symbol']

    # Export
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    filepath = '../docs/' + ratio_name + '.csv'
    df.to_csv(filepath, index=0)

    return df


def merge_ratio(df, all_prices, ratio_name):
    """
    Function to merge ratio with all prices
    """

    # Metric Dictionary
    dic = {'pe_ratio': 'eps_ttm',
            'price_to_book_value': 'book_value_ttm'}

    # Field name
    field_name = dic[ratio_name]

    # Rename columns, convert column to datetime and keep only records where date > 2017-01-01
    df.columns = ['timestamp_merge', ratio_name, 'symbol']
    df['timestamp_merge'] = pd.to_datetime(df['timestamp_merge'])
    df = df[df['timestamp_merge'] > '2014-01-01']

    # Convert all prices column to datetime
    all_prices['just_date_merge'] = pd.to_datetime(all_prices['just_date'])

    # Merge both dataframes
    merge_df = pd.merge(all_prices,
                        df,
                        left_on = ['just_date_merge', 'symbol'],
                        right_on = ['timestamp_merge', 'symbol'],
                        how='left')

    # Calculate EPS TTM based on weekly PE Ratios
    merge_df[field_name] = merge_df['close_price'] / merge_df[ratio_name]
    merge_df[field_name] = merge_df[field_name].round(3)

    # Since we have only Weekly Value we can Forward/Backward Fill the EPS TTM
    merge_df[field_name] = merge_df.groupby('symbol').ffill()[field_name]
    merge_df[field_name] = merge_df.groupby('symbol').bfill()[field_name]

    # Calculate PE Ratio with EPS TTM and round numbers
    merge_df[ratio_name] = merge_df['close_price'] / merge_df[field_name]
    merge_df[ratio_name] = merge_df[ratio_name].round(3)

    # Drop columns
    merge_df.drop(['just_date_merge', 'timestamp_merge'], inplace=True, axis=1)

    # Export
    merge_df.to_csv('../docs/' + field_name + '.csv', index=0)

    return merge_df

def pe_analysis(prices_pe):

    # Calculate Average Market Cap by Industry/Sector overtime
    daily_pe_ratio_mean = prices_pe[['just_date', 'industry', 'sector', 'pe_ratio']].groupby(['just_date', 'industry', 'sector']).mean()['pe_ratio']
    daily_pe_ratio_std = prices_pe[['just_date', 'industry', 'sector', 'pe_ratio']].groupby(['just_date', 'industry', 'sector']).std()['pe_ratio']

    # Convert to Data Frame
    daily_pe_ratio_mean = daily_pe_ratio_mean.reset_index()
    daily_pe_ratio_std = daily_pe_ratio_std.reset_index()

    # Rename Columns
    daily_pe_ratio_mean.columns = ['just_date', 'industry', 'sector', 'avg_pe_ratio']
    daily_pe_ratio_std.columns = ['just_date', 'industry', 'sector', 'std_pe_ratio']

    # Merge with main
    daily_pe_stats = pd.merge(daily_pe_ratio_mean, daily_pe_ratio_std, on=['just_date', 'industry', 'sector'])
    daily_pe_stats = daily_pe_stats.dropna()

    # Merge with original
    prices_pe['just_date'] = pd.to_datetime(prices_pe['just_date'])
    daily_pe_stats['just_date'] = pd.to_datetime(daily_pe_stats['just_date'])
    prices_pe = pd.merge(prices_pe, daily_pe_stats, how='left', on=['just_date', 'industry', 'sector'])

    # See how many STDs a PE Ratio is far from mean
    prices_pe['pe_ratio_std_diff'] = (prices_pe['avg_pe_ratio'] - prices_pe['pe_ratio']) / prices_pe['std_pe_ratio']

    return prices_pe

def main_ratio(all_prices, ratio_name, full_refresh=False):

    # If a full refresh is not necessary
    if full_refresh == False:

        # Read existing PE Ratios and merge it with all prices
        df = pd.read_csv('../docs/' + ratio_name + '.csv')
        df = df.dropna()

        # Add to All Prices
        df = merge_ratio(df, all_prices, ratio_name)

        if ratio_name == 'pe_ratio':
            df = pe_analysis(df)

    else:
        # Extract from website and
        df = zacks_extract(ratio_name)

        # Add to All Prices
        df = merge_ratio(df, all_prices, ratio_name)

        if ratio_name == 'pe_ratio':
            df = pe_analysis(df)

    return df
