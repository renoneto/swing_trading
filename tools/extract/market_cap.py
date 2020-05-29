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

    return pe_and_prices
