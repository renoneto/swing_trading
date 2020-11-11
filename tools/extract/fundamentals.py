from requests import Session
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

from datetime import datetime

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '\
                         'AppleWebKit/537.36 (KHTML, like Gecko) '\
                         'Chrome/75.0.3770.80 Safari/537.36'}

def search_symbol(symbol):
    """
    Search for symbol's link in Simply Wall Street
    """
    # Create Session
    s = Session()

    # Add headers
    s.headers.update(HEADERS)

    # JSON Key Field
    url = f'https://api.simplywall.st/api/search/{symbol}'

    # Request and transform response in json
    screener = s.get(url)
    json = screener.json()

    if len(json) != 0:
        # Stock URL
        stock_url = json[0]['url']

    else:
        stock_url = 'not found'

    return stock_url

def extract_all_urls(stocks_path='../docs/my_stocks.feather'):
    """
    Create file with urls to call api
    """

    # Read csv with stocks
    my_stocks_df = pd.read_feather(stocks_path)

    # Create List with stocks
    my_stocks_list = list(my_stocks_df['symbol'].unique())

    # Find all urls and store in a dataframe
    results = []
    for stock in my_stocks_list:
        print(stock)
        url = search_symbol(stock)
        results.append([stock, url])

    # Convert into a dataframe
    results_df = pd.DataFrame(results, columns=['symbol', 'url'])

    # Export to csv
    results_df.to_csv('../docs/simplywallurls.csv', index=0)

    return results_df

def symbol_data(stock_url):
    """
    Extract data from Simply Wall Steet
    """
    # Create Session
    s = Session()

    # Add headers
    s.headers.update(HEADERS)

    # JSON Key Field
    metrics_url = f'https://api.simplywall.st/api/company{stock_url}?include=info%2Cscore%2Cscore.snowflake%2Canalysis.extended.raw_data%2Canalysis.extended.raw_data.insider_transactions&version=2.0'

    # Request and transform response in json
    screener = s.get(metrics_url)

    # check status
    if screener.status_code == 200:
        json = screener.json()
    else:
        json = 'not found'

    return json

def extract_values(json_response, symbol):
    """
    Extract important values from json_response for each symbol
    """

    # Define important fields
    fields_dictionary = {'total_assets': 'total_assets',
                        'total_current_assets': 'total_ca',
                        'cash_st_investments': 'cash_st_invest',
                        'total_receivables': 'total_receiv',
                        'inventory': 'inventory',
                        'net_property_plant_equip': 'nppe',
                        'short_term_debt': 'current_port_capital_leases',
                        'total_current_liabilities': 'total_cl',
                        'long_term_debt': 'lt_debt',
                        'total_liabilities': 'total_liabilities',
                        'total_equity': 'total_equity',
                        'accounts_payable': 'ap',
                        'total_revenue_ttm': 'total_rev',
                        'ebt_ttm':'ebt',
                        'ebitda_ttm': 'ebitda',
                        'ebit_ttm': 'ebit',
                        'pre_tax_income': 'earning_co',
                        'gross_profit_ttm': 'gross_profit',
                        'net_income_ttm': 'ni',
                        'g_a_expense_ttm': 'g_a_expense',
                        'income_tax_ttm': 'income_tax',
                        'interest_exp_ttm': 'interest_exp',
                        'basic_eps_ttm': 'basic_eps',
                        'net_oper_cf_ttm': 'cash_oper',
                        'net_investing_cf_ttm': 'cash_f_investing',
                        'net_financing_cf_ttm': 'cash_f_financing',
                        'levered_fcf_ttm': 'levered_fcf',
                        'capex_ttm': 'capex',
                        'beta_5yr': 'beta_5yr'}

    # Check response code
    if json_response != 'not found':
        # Get to fields that really matter
        assets = json_response['data']['analysis']['data']['extended']['data']['raw_data']['data']['past']

        # check if there's data
        if len(assets) > 0:

            # Extract Available dates
            dates = assets.keys()

            # Create empty list to store results
            results = []
            # Create first row with headers
            headers = []
            headers.append('date')
            headers.append('symbol')
            [headers.append(row) for row in list(fields_dictionary.keys())]
            results.append(headers)

            # For each date in dates
            for date in dates:

                # Create Temporary list to append results for each date
                temp_results = []
                temp_results.append(date)
                temp_results.append(symbol)

                # See available keys - not all fields are available all the time
                available_keys = assets[date].keys()

                # For field in list of fields to pull
                for field in fields_dictionary.values():

                    # if field is available
                    if field in available_keys:

                        # create value and append that
                        value = assets[date][field]['value']
                        temp_results.append(value)

                    # if field doesn't exist then append NaN
                    else:
                        temp_results.append(np.nan)

                # Append to results
                results.append(temp_results)

            return results

        else:

            return 'not found'

def extract_fundamentals(update_urls=False, urls_path='../docs/simplywallurls.csv'):
    """
    Function to extract all fundamentals for all stocks
    """

    # Check if we need to update list of urls
    if update_urls == False:

        # Read csv with stocks
        urls_df = pd.read_csv(urls_path, header=0)

    else:
        urls_df = extract_all_urls()

    # Create variable with total number of stocks so we can track progress
    length = len(urls_df)

    # create list to store results
    results = []

    # Loop through symbols
    for index, row in urls_df.iterrows():

        # Extract values
        stock_url = row['url']
        symbol = row['symbol']

        # Print progress
        print( str( round((((index + 1) / length) * 100), 2)) + '% Complete', symbol)

        # If url is different than 'not found'
        if row['url'] != 'not found':

            # Extract json with values
            stock_json_response = symbol_data(stock_url)

            # Check if there's data
            if stock_json_response != 'not found':
                # Keep onlu relevant values
                stock_numbers = extract_values(stock_json_response, symbol)
                # Add that to results list
                results.append(stock_numbers)

    # Transform results into a dataframe, first create a list where every row is one record for each stock
    to_df_list = [i for stock in results for i in stock]
    # Convert it to a dataframe - dropping duplicates for headers (not the best solution)
    df = pd.DataFrame(to_df_list, columns=to_df_list[0]).drop_duplicates()
    # Remove first row with headers
    df = df[1:]

    # Export that
    df.to_csv('../docs/my_stocks_fundamentals.csv', index=0)

    return df

def update_fundamental_dates():
    """
    Function to update fundamental data from Simply Wall Street
    """
    # Import Fundamental Data and Earnings
    df_fund = pd.read_csv('../docs/my_stocks_fundamentals.csv')
    df_earnings = pd.read_csv('../docs/earnings.csv')

    # Remove duplicates from df_earnings
    df_earnings['earnings_date'] = pd.to_datetime(df_earnings['earnings_date']).dt.date
    df_earnings = df_earnings.drop_duplicates(keep='first', subset=['symbol', 'earnings_date'])

    # Create columns with previous Qs numbers
    # First we need to define the relevant columns
    relevant_columns = list(set(list(df_fund.columns)) - set(['date', 'symbol']))
    relevant_columns = ['basic_eps_ttm', 'net_income_ttm', 'net_oper_cf_ttm', 'total_revenue_ttm']

    # Loop through columns and create a new column with previous numbers
    for column in relevant_columns:
        for i in range(1,17):
            number = i * -1
            df_fund[f'{column}_{i}Q'] = df_fund.groupby('symbol')[column].shift(number)

    # Now we need to pull data from earnings, because we need to tell exactly when all the data was available
    # Transform dataframes
    df_fund['date_str'] = df_fund['date'].astype(str).str[:-3]
    df_fund['earnings_quarter'] = pd.to_datetime(df_fund['date_str'], unit='s')

    # Figure out the correct dates in which earnings was released
    df_earnings['key'] = 0
    df_fund['key'] = 0

    # Merge all together, looking at all possibilities
    clean_df = pd.merge(df_earnings, df_fund, on=['symbol', 'key'])
    clean_df['earnings_quarter'] = pd.to_datetime(clean_df['earnings_quarter']).dt.date
    clean_df['earnings_date'] = pd.to_datetime(clean_df['earnings_date']).dt.date
    clean_df['difference'] = (clean_df['earnings_date'] - clean_df['earnings_quarter']).dt.days
    check = clean_df[(clean_df['difference'] >= 0)].groupby(['symbol','earnings_quarter']).min()['difference'].reset_index()
    final = pd.merge(clean_df, check, on=['symbol', 'earnings_quarter', 'difference'])

    # Drop columns
    final.drop('date', axis=1, inplace=True)

    # Export to csv
    final.to_feather('../docs/my_stock_fundamentals_correct_dates.feather')

    return final

def update_prices(fundamentals, all_prices):
    """
    Function to Update Prices using Fundamental Data
    """

    # Convert date columns to datetime
    fundamentals['earnings_date'] = pd.to_datetime(fundamentals['earnings_date'])
    fundamentals['earnings_date'] = fundamentals['earnings_date'].dt.date
    all_prices['just_date'] = pd.to_datetime(all_prices['just_date'])
    all_prices['just_date'] = all_prices['just_date'].dt.date

    # Merge both dataframes
    merge_df = pd.merge(all_prices[['just_date', 'symbol', 'close_price', 'industry']], fundamentals, left_on=['just_date', 'symbol'], right_on=['earnings_date', 'symbol'], how='left')

    # EPS TTM Last 60, 120, 180, 240, 300
    symbol_grouped = merge_df.groupby('symbol')

    for idx, i in enumerate([60, 120, 180, 240]):
        # Shift Columns - Calculate Past EPS TTM
        merge_df[f'past_eps_ttm_{i}'] = symbol_grouped['basic_eps_ttm'].shift(i)

    # Calculate Growth of EPS
    for idx, i in enumerate([60, 120, 180, 240]):
        # Compare EPS TTMs - Current with Previous - Previous with Previous of Previous and so on
        for idx2, y in enumerate([60, 120, 180, 240]):
            # If difference is one it means that y comes right after i.
            if idx2 - idx == 1:
                # Calculate Growth
                merge_df[f'eps_ttm_difference_{i}_{y}'] = (merge_df[f'past_eps_ttm_{i}'] - merge_df[f'past_eps_ttm_{y}']) / merge_df[f'past_eps_ttm_{y}'].abs()

    # Figure out fields that matter
    columns_to_fill = merge_df.columns[merge_df.isna().any()].tolist()
    columns_to_fill = list(set(columns_to_fill) - set(['high_price', 'low_price', 'open_price', 'close_price', 'volume', 'earnings_date', 'industry']))

    # Forward Fill fields
    merge_df[columns_to_fill] = merge_df.groupby('symbol').ffill()[columns_to_fill]

    # Calculate Current - EPS TTM 60 (For simplicity, I chose to keep it out of the loop above)
    merge_df['eps_ttm_difference_0_60'] = (merge_df['basic_eps_ttm'] / merge_df['past_eps_ttm_60']) - 1

    # No. of shares
    merge_df['shs'] = merge_df['net_income_ttm'] / merge_df['basic_eps_ttm']

    # Calculate PE Ratio
    merge_df['pe_ratio'] = merge_df['close_price'] / merge_df['basic_eps_ttm']

    # Calculate Book Value - Price to Book Ratio
    merge_df['book_value'] = merge_df['total_assets'] - merge_df['total_liabilities']
    merge_df['book_value_per_share'] = merge_df['book_value'] / merge_df['shs']
    merge_df['pb_ratio'] = merge_df['close_price'] / merge_df['book_value_per_share']

    # Calculate Debt-to-Equity Ratio
    merge_df['debt_equity_ratio'] = merge_df['long_term_debt'] / merge_df['total_equity']

    # Calculate Earnings Growth - PEG Ratio
    merge_df['earnings_growth_rate'] = 100 * (((merge_df['basic_eps_ttm'] / merge_df['basic_eps_ttm_16Q']) ** (1/4)) - 1)
    merge_df['peg_ratio'] = merge_df['pe_ratio'] / merge_df ['earnings_growth_rate']

    # Price to Sales Ratio
    merge_df['sales_per_share'] = merge_df['total_revenue_ttm'] / merge_df['shs']
    merge_df['price_to_sales_ratio'] = merge_df['close_price'] / merge_df['sales_per_share']

    # Calculate Market Cap
    merge_df['market_cap_calc'] = merge_df['shs'] * merge_df['close_price']

    # Previous Values for Ratios
    #for idx, i in enumerate([60, 120, 180, 240]):

        # List of Ratios
        #for ratio in ['pe_ratio', 'pb_ratio', 'debt_equity_ratio', 'peg_ratio', 'price_to_sales_ratio']:

            # Shift Columns - Calculate Past Ratios
            #merge_df[f'{ratio}_{i}'] = merge_df.groupby('symbol')[ratio].shift(i)

    # Drop close price column
    merge_df.drop('close_price', axis=1, inplace=True)

    # Add Industry Median Value
    #industry_df = merge_df.groupby(['just_date', 'industry']).median()[['pe_ratio', 'pb_ratio', 'debt_equity_ratio', 'peg_ratio', 'price_to_sales_ratio']]
    #industry_df = industry_df.reset_index()
    #industry_df.columns = ['just_date', 'industry', 'pe_ratio_industry', 'pb_ratio_industry', 'debt_equity_ratio_industry', 'peg_ratio_industry', 'price_to_sales_ratio_industry']

    # Merge that with merge_df
    #merge_df = pd.merge(merge_df, industry_df, on=['just_date', 'industry'], how='left')

    # Drop industry column
    merge_df.drop('industry', axis=1, inplace=True)

    # Export
    #merge_df.to_csv('../docs/fundamental_prices.csv', index=0)

    return merge_df

def main_fundamentals(all_prices, indicators):
    """
    Main function to update Fundamental data according to earnings date releases
    """

    # Create fundamentals_dataframe
    fundamentals = pd.read_feather('../docs/my_stock_fundamentals_correct_dates.feather')

    # Merge fundemantals with all prices
    fundamentals_prices = update_prices(fundamentals, all_prices)

    # Merge Fundamentals with Indicators
    indicators['just_date'] = pd.to_datetime(indicators['just_date'])
    indicators['just_date'] = indicators['just_date'].dt.date
    fundamentals_prices['just_date'] = pd.to_datetime(fundamentals_prices['just_date'])
    fundamentals_prices['just_date'] = fundamentals_prices['just_date'].dt.date
    indicators_fundamentals = pd.merge(indicators, fundamentals_prices, on=['just_date', 'symbol'], how='left')

    return indicators_fundamentals

