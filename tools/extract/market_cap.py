import pandas as pd

def market_cap(pe_and_prices):
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
    pe_and_prices.to_csv('../docs/pe_and_prices_market_cap.csv', index=0)

    # Market Cap Analysis
    pe_and_prices = market_cap_analysis(pe_and_prices)

    return pe_and_prices

def market_cap_analysis(pe_and_prices):

    # Calculate Average Market Cap by Industry/Sector overtime
    daily_market_cap_sum = pe_and_prices[['just_date', 'industry', 'sector', 'market_cap']].groupby(['just_date', 'industry', 'sector']).sum()['market_cap']
    daily_market_cap_count = pe_and_prices[['just_date', 'industry', 'sector', 'market_cap']].groupby(['just_date', 'industry', 'sector']).count()['market_cap']
    daily_average_market_cap = daily_market_cap_sum / daily_market_cap_count

    # Reset Index
    daily_average_market_cap = daily_average_market_cap.reset_index()
    daily_market_cap_count = daily_market_cap_count.reset_index()
    daily_market_cap_sum = daily_market_cap_sum.reset_index()

    # Rename columns
    daily_average_market_cap.columns = ['just_date', 'industry', 'sector', 'avg_market_cap']
    daily_market_cap_count.columns = ['just_date', 'industry', 'sector', 'count_market_cap']
    daily_market_cap_sum.columns = ['just_date', 'industry', 'sector', 'sum_market_cap']

    # Create master and drop NaNs
    daily_market_cap = pd.merge(daily_average_market_cap, daily_market_cap_count, on=['just_date', 'industry', 'sector'])
    daily_market_cap = pd.merge(daily_market_cap, daily_market_cap_sum, on=['just_date', 'industry', 'sector'])
    daily_market_cap = daily_market_cap.dropna()

    # Merge with original
    pe_and_prices['just_date'] = pd.to_datetime(pe_and_prices['just_date'])
    daily_market_cap['just_date'] = pd.to_datetime(daily_market_cap['just_date'])
    pe_and_prices = pd.merge(pe_and_prices, daily_market_cap, how='left', on=['just_date', 'industry', 'sector'])

    # Calculate % of Market Cap
    pe_and_prices['perc_market_cap'] = pe_and_prices['market_cap'] / pe_and_prices['sum_market_cap']

    return pe_and_prices
