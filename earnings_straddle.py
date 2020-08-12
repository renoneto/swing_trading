from yahoo_earnings_calendar import YahooEarningsCalendar
from yahooquery import Ticker
import pandas as pd
from datetime import datetime, timedelta

# Create instance of yec
yec = YahooEarningsCalendar()

# Define Start/End Dates and Pull list of stocks with upcoming earnings
start = datetime.today() + timedelta(days=15)
end = datetime.today() + timedelta(days=25)
earnings = yec.earnings_between(start, end)

# Extract symbols from results
screener_symbols = [dic['ticker'] for dic in earnings]

# Pull current prices
screener = Ticker(screener_symbols)
today_price = screener.price

# Prices DataFrame
prices_df = pd.DataFrame(today_price).T.reset_index()
prices_df.rename({'regularMarketPrice':'stock_price'}, inplace=True, axis=1)

# Create empty list to store filtered stocks
filtered_stocks = []

# Go through stocks and check if price is between 5/100
for stock in today_price.keys():

    # Dealing with stocks with no results
    if type(today_price[stock]) != str:

        # Extract price from dictionary
        if 'regularMarketPrice' in today_price[stock].keys():
            price = today_price[stock]['regularMarketPrice']

            # If price between parameters then add to list
            if price >= 5 and price <= 100:
                filtered_stocks.append(stock)

# Extract option chains for filtered stocks
filtered_screener = Ticker(filtered_stocks)
option_chains = filtered_screener.option_chain

# Reset Index
option_chains_df = option_chains.reset_index()

# Create Rank to find options closes to current price
option_chains_df['rank'] = option_chains_df.groupby(['symbol', 'expiration', 'optionType', 'inTheMoney']).rank()['strike']

# Find total count of contracts with the same expiration date, option type and in the money value
total_count = option_chains_df.groupby(['symbol', 'expiration', 'optionType', 'inTheMoney']).max()['rank'].reset_index(name='total_count')

# Merge result back with main dataframe and calculate the difference between rank and total count
option_chains_df = pd.merge(option_chains_df, total_count, on=['symbol', 'expiration', 'optionType', 'inTheMoney'])
option_chains_df['difference'] = option_chains_df['total_count'] - option_chains_df['rank']

# Filter for the closest 2 option contracts (on both sides) to current price
option_chains_df = option_chains_df[((option_chains_df['difference'] < 2) & (option_chains_df['inTheMoney'] == True) & (option_chains_df['optionType'] == 'calls')) |
                                    ((option_chains_df['rank'] < 3) & (option_chains_df['inTheMoney'] == False) & (option_chains_df['optionType'] == 'calls')) |
                                    ((option_chains_df['rank'] < 3) & (option_chains_df['inTheMoney'] == True) & (option_chains_df['optionType'] == 'puts')) |
                                    ((option_chains_df['difference'] < 2) & (option_chains_df['inTheMoney'] == False) & (option_chains_df['optionType'] == 'puts'))]

# Create earnings dataframe
earnings_df = pd.DataFrame(earnings)
earnings_df['startdatetime'] = pd.to_datetime(earnings_df['startdatetime']).dt.tz_convert(None)
earnings_df = earnings_df[['ticker', 'startdatetime']]
earnings_df.columns = ['ticker', 'earnings_date']

# Pull earning dates from dataframe
option_chains_df = pd.merge(option_chains_df, earnings_df[['ticker', 'earnings_date']], left_on='symbol', right_on='ticker')

# Calculate difference in number of days between expiration and earnings date
option_chains_df['days_until_earnings'] = (option_chains_df['earnings_date'] - option_chains_df['expiration']).dt.days

# Create new dataframe to find the two closest expiration dates
# Get unique combination of symbol and days until earnings
rank_days = option_chains_df[['symbol', 'days_until_earnings']].drop_duplicates()

# Create boolean field looking whether the number of days is positive or negative
rank_days.loc[rank_days['days_until_earnings'] < 0, 'below_zero'] = True
rank_days.loc[rank_days['days_until_earnings'] > 0, 'below_zero'] = False

# Create rank based on symbol and this boolean classification
rank_days['rank'] = rank_days.groupby(['symbol', 'below_zero']).rank(ascending=False)['days_until_earnings']

# Calculate max rank and min rank for the combination of below zero and symbol
max_rank = rank_days.groupby(['symbol', 'below_zero']).max()['rank'].reset_index()
max_rank.columns = ['symbol', 'below_zero', 'max_rank']
min_rank = rank_days.groupby(['symbol', 'below_zero']).min()['rank'].reset_index()
min_rank.columns = ['symbol', 'below_zero', 'min_rank']

# Merge with rank_days
rank_days = pd.merge(rank_days, max_rank, on=['symbol', 'below_zero'])
rank_days = pd.merge(rank_days, min_rank, on=['symbol', 'below_zero'])

# If below zero (expiration date > earnings date) then we need to pull the min rank, which will be the first
# contract after earnings date
# If above zero (expiration date < earnings date) then we need to pull the max rank, because we might have two
# contracts that come before earnings and we want the second one (or the max rank)
rank_days = rank_days[((rank_days['below_zero'] == False) & (rank_days['rank'] == rank_days['max_rank'])) |
                      ((rank_days['below_zero'] == True) & (rank_days['rank'] == rank_days['min_rank']))]

# Merge back with master
option_chains_df = pd.merge(option_chains_df, rank_days[['symbol', 'days_until_earnings']], on=['symbol', 'days_until_earnings'])
option_chains_df = option_chains_df.drop(['rank', 'total_count', 'difference'], axis=1)

# Merge with prices and export
option_chains_df = pd.merge(option_chains_df, prices_df[['symbol', 'stock_price']], on='symbol')

# Open Interest above 100
option_chains_df = option_chains_df[option_chains_df['openInterest'] > 100]

# Spread Ratio below 0.30
option_chains_df['spread_ratio'] = (option_chains_df['ask'] - option_chains_df['bid']) / option_chains_df['ask']
option_chains_df = option_chains_df[option_chains_df['spread_ratio'] < 0.3]

# Create unique ID to count number of combinations with Call and Put
option_chains_df['unique_id'] = option_chains_df['symbol'] + option_chains_df['expiration'].astype(str) + option_chains_df['strike'].astype(str)
unique_id_count = option_chains_df.groupby('unique_id').size().reset_index()
unique_id_count.columns = ['unique_id', 'count']

# Merge back with main df
option_chains_df = pd.merge(option_chains_df, unique_id_count, on='unique_id')

# Keep only unique ids where count == 2 and drop columns
option_chains_df = option_chains_df[option_chains_df['count'] == 2]
option_chains_df = option_chains_df.drop(['count', 'unique_id'], axis = 1)
option_chains_df.to_csv('kent_is_too_excited.csv', index=0)
