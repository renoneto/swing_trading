import requests
from bs4 import BeautifulSoup
import pandas as pd

def extract_eps():
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

    # For every single stock, do the following
    for idx, stock in enumerate(list_of_stocks):
        print((idx+1)/lenght)
        url = f'https://ycharts.com/charts/fund_data.json?securities=include%3Atrue%2Cid%3A{stock}%2C%2C&calcs=include%3Atrue%2Cid%3Aeps%2C%2C'
        response = requests.get(url)
        if len(response.text) > 64:
            json = response.json()
            for i in json['chart_data'][0][0]['raw_data']:
                i.append(stock)
                list_to_append.append(i)

    # Create dataframe with results
    eps_df = pd.concat(results_list)
    eps_df.columns = ['timestamp', 'eps', 'symbol']

    # Export
    eps['timestamp'] = pd.to_datetime(eps['timestamp'], unit='ms')
    eps.to_csv('../docs/eps.csv', index=0)

def merge_eps(eps, eps_location='../docs/eps.csv'):
    """
    Function to merge EPS with prices, calculate TTM EPS and PE Ratio
    """

    # Reset Index so we can group by index
    eps = eps.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    # Calculate TTM EPS
    ttm = eps.groupby('symbol')['eps'].rolling(4).sum().reset_index()
    ttm.columns = ['symbol', 'date', 'ttm_eps']

    # Merge both
    eps = pd.merge(eps, ttm[['ttm_eps']], left_index=True, right_index=True)

    # Read prices
    prices_df = pd.read_csv('../docs/prices.csv')

    # Concatenate DataFrames
    pe_ratio = pd.concat([prices_df, eps]).sort_values(['symbol', 'timestamp'])
    pe_ratio['timestamp'] = pd.to_datetime(pe_ratio['timestamp'])
    pe_ratio = pe_ratio.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    # Forward Fill numbers
    pe_ratio['eps'] = pe_ratio.groupby('symbol').ffill()['eps']
    pe_ratio['ttm_eps'] = pe_ratio.groupby('symbol').ffill()['ttm_eps']
    pe_ratio.dropna(subset=['interval'], inplace=True)

    # Calculate PE Ratio
    pe_ratio['pe_ratio'] = pe_ratio['close_price'] / pe_ratio['ttm_eps']

    # Export file
    pe_ratio[['timestamp', 'eps', 'ttm_eps', 'pe_ratio']].to_csv('../docs/eps.csv', index=0)

    return pe_ratio

def main_pe(full_refresh=False):

    if full_refresh == False:
        eps = pd.read_csv('../docs/eps.csv')

        if 'ttm_eps' in eps.columns:
            eps.drop('ttm_eps', axis=1, inplace=True)

        pe_ratio = merge_eps(eps)

    else:
        eps = extract_eps()
        pe_ratio = merge_eps(eps)

    return pe_ratio
