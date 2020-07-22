import pandas as pd
import numpy as np

def main_market_cap(pe_and_prices):
    """
    Function to Calculate Market Cap over time assuming that number of shares doesn't change
    """
    # Remove B letter from Market Cap and convert string field to number
    pe_and_prices['market_cap_no'] = pe_and_prices['market_cap'].str.replace('B', '')
    pe_and_prices['market_cap_no'] = pd.to_numeric(pe_and_prices['market_cap_no'])

    # Figure out last day of date data (our starting point)
    max_date = pe_and_prices.iloc[-1]['just_date']
    market_cap = pe_and_prices[pe_and_prices['just_date'] == max_date]

    # Calculate Number of Shares today
    market_cap['shs_outstanding'] = market_cap['market_cap_no'] / market_cap['close_price']

    # Keep only columns that we need
    market_cap = market_cap[['just_date', 'symbol', 'shs_outstanding']]

    # Merge original prices with last day's number of shares outstanding
    pe_and_prices = pd.merge(pe_and_prices, market_cap, on=['just_date', 'symbol'], how='left')

    # Backward Fill the number of shares and calculate the moving market cap using the close price
    pe_and_prices['shs_outstanding'] = pe_and_prices.groupby('symbol').bfill()['shs_outstanding']
    pe_and_prices['market_cap'] = pe_and_prices['shs_outstanding'] * pe_and_prices['close_price']

    # Market Cap Categories
    pe_and_prices['cap_cat'] = pd.cut(pe_and_prices['market_cap'], bins = [0, 2, 10, 10000000], labels=['small', 'mid', 'big'])

    # Drop some columns
    pe_and_prices.drop(['market_cap_no', 'shs_outstanding'], inplace=True, axis=1)

    # Export
    #pe_and_prices.to_csv('../docs/pe_and_prices_market_cap.csv', index=0)

    # Market Cap Analysis
    pe_and_prices = market_cap_analysis(pe_and_prices)

    # Weighted Returns
    #pe_and_prices = weighted_returns(pe_and_prices)
    pe_and_prices.to_csv('pe_prices_market_cap_analysis.csv', index=0)

    return pe_and_prices

def market_cap_analysis(pe_and_prices):

    # Calculate Average Market Cap by Industry/Sector overtime
    daily_market_cap_sum = pe_and_prices[['just_date', 'industry', 'sector', 'market_cap']].groupby(['just_date', 'industry', 'sector']).sum()['market_cap']
    daily_market_cap_std = pe_and_prices[['just_date', 'industry', 'sector', 'market_cap']].groupby(['just_date', 'industry', 'sector']).std()['market_cap']
    daily_market_cap_count = pe_and_prices[['just_date', 'industry', 'sector', 'market_cap']].groupby(['just_date', 'industry', 'sector']).count()['market_cap']
    daily_average_market_cap = daily_market_cap_sum / daily_market_cap_count

    # Reset Index
    daily_average_market_cap = daily_average_market_cap.reset_index()
    daily_market_cap_count = daily_market_cap_count.reset_index()
    daily_market_cap_sum = daily_market_cap_sum.reset_index()
    daily_market_cap_std = daily_market_cap_std.reset_index()

    # Rename columns
    daily_average_market_cap.columns = ['just_date', 'industry', 'sector', 'avg_market_cap']
    daily_market_cap_count.columns = ['just_date', 'industry', 'sector', 'count_market_cap']
    daily_market_cap_sum.columns = ['just_date', 'industry', 'sector', 'sum_market_cap']
    daily_market_cap_std.columns = ['just_date', 'industry', 'sector', 'std_market_cap']

    # Create master and drop NaNs
    daily_market_cap = pd.merge(daily_average_market_cap, daily_market_cap_count, on=['just_date', 'industry', 'sector'])
    daily_market_cap = pd.merge(daily_market_cap, daily_market_cap_sum, on=['just_date', 'industry', 'sector'])
    daily_market_cap = pd.merge(daily_market_cap, daily_market_cap_std, on=['just_date', 'industry', 'sector'])
    daily_market_cap = daily_market_cap.dropna()

    # Merge with original
    pe_and_prices['just_date'] = pd.to_datetime(pe_and_prices['just_date'])
    daily_market_cap['just_date'] = pd.to_datetime(daily_market_cap['just_date'])
    pe_and_prices = pd.merge(pe_and_prices, daily_market_cap, how='left', on=['just_date', 'industry', 'sector'])

    # Calculate % of Market Cap
    pe_and_prices['perc_market_cap'] = pe_and_prices['market_cap'] / pe_and_prices['sum_market_cap']

    # Security pct change
    pct_change = pe_and_prices.groupby(['symbol'])['close_price'].pct_change().reset_index()
    pct_change.columns = ['index', 'pct_change']
    pe_and_prices = pd.merge(pe_and_prices, pct_change[['pct_change']], left_index=True, right_index=True)

    # Calculated Weighted Perct Change based on Industry/Sector/Market Cap
    pe_and_prices['weighted_change'] = pe_and_prices['pct_change'] * pe_and_prices['perc_market_cap']
    weighted_returns = pe_and_prices.groupby(['just_date', 'industry', 'sector']).sum()['weighted_change'].reset_index()
    weighted_returns.columns = ['just_date', 'industry', 'sector', 'index_daily_return']
    weighted_returns['index_daily_return'] = weighted_returns['index_daily_return'] + 1

    # Merge it back to symbols
    pe_and_prices = pd.merge(pe_and_prices, weighted_returns, on=['just_date', 'industry', 'sector'], how='left')

    return pe_and_prices

def weighted_returns(pe_and_prices):

    # Create unique id
    pe_and_prices['unique_id'] = pe_and_prices['industry'] + pe_and_prices['sector']

    # Create unique id to loop through
    weighted_returns = pe_and_prices[['just_date', 'unique_id', 'index_daily_return']]
    weighted_returns = weighted_returns.drop_duplicates()

    # Create Empty List to store results
    results_list = []

    for unique_id in weighted_returns['unique_id'].unique():

        # Filter only for a given unique id
        sector_df = weighted_returns[weighted_returns['unique_id'] == unique_id]

        # Calculate Past Returns
        for i in [2, 3, 4, 5, 7, 10, 15, 20, 30, 60, 90, 120]:
            sector_df[f'moving_{i}d_return_index'] = (sector_df['index_daily_return']).rolling(window=i).apply(np.prod, raw=True)

        # Calculate min daily moves
        for i in [30, 60, 90]:
            sector_df[f'moving_{i}d_return_mean_index'] = sector_df['index_daily_return'].rolling(window=i).mean()
            sector_df[f'moving_{i}d_return_std_index'] = (sector_df['index_daily_return'] - 1).rolling(window=i).std()
            sector_df[f'moving_{i}d_min_return_index'] = sector_df[f'moving_{i}d_return_mean_index'] - (2 * sector_df[f'moving_{i}d_return_std_index'])

        # Append results to list
        results_list.append(sector_df)

    # Concat Results and return
    weighted_returns = pd.concat(results_list)

    # Merge back with all stocks
    pe_and_prices.drop('index_daily_return', axis=1, inplace=True)
    pe_and_prices = pd.merge(pe_and_prices, weighted_returns, on=['just_date', 'unique_id'], how='left')

    return pe_and_prices
