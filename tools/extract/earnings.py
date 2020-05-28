import pandas as pd
from bs4 import BeautifulSoup
import requests
from requests import Session
from datetime import datetime, timedelta
import time

headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '\
                     'AppleWebKit/537.36 (KHTML, like Gecko) '\
                     'Chrome/75.0.3770.80 Safari/537.36'}

def extract_earnings(start_date='2015-01-01', future_days=60, threshold_days=20):
    """
    Function to Extract earnings dates from a website. It extracts all symbols.
    start_date = Extracting starting in start_date
    future_days = Extract until today + future_days

    It will extract based on the max date found on the existing file, the threshold is set on the function
    """

    # Read file
    earnings_df = pd.read_csv('../docs/earnings.csv')

    # Calculate max date
    earnings_df['earnings_date'] = pd.to_datetime(earnings_df['earnings_date'])
    difference = earnings_df['earnings_date'].max() - datetime.today()

    # See if difference is less than threshold
    if difference.days <= 20:

        # Define Session and Headers
        s = Session()
        s.headers.update(headers)

        # Create list to store results
        earnings_dates = []

        # Define Start, End and Current Dates
        current_date = datetime.today() + timedelta(days=future_days)
        current_date_str = current_date.strftime("%Y-%m-%d")

        # Do this while current date is greater or less than start_date
        while current_date_str != start_date:

            # Calculate current_date minus one day
            current_date = current_date - timedelta(days=1)
            current_date_str = current_date.strftime("%Y-%m-%d")

            # Create url to pull data
            earnings_url_base = 'https://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=eventscal&date='
            earnings_timestamp = int(time.mktime(time.strptime((current_date_str + ' 00:00:00'), '%Y-%m-%d %H:%M:%S')))
            earnings_url = earnings_url_base + str(earnings_timestamp)

            # Pull Data
            earnings_response = s.get(earnings_url)
            earnings_json = BeautifulSoup(earnings_response.text, 'html.parser')

            # Create list comprehension
            list_comp = [[a.text, current_date_str] for a in earnings_json.find_all('a') if a.text != '']
            [earnings_dates.append(i) for i in list_comp]

            print('Extracting ' + current_date_str + ' earnings.')

        # Export information
        earnings_df = pd.DataFrame(earnings_dates, columns = ['symbol', 'earnigs_date'])
        earnings_df.to_csv('../docs/earnings.csv', index=0)

    # If a refresh is not necessary then just read existing file
    else:
        # Read file
        earnings_df = pd.read_csv('../docs/earnings.csv')

    return earnings_df

def add_earnings(trades):

    # Create Earnings Dataframe
    earnings_df = extract_earnings()

    # Read Earnings
    earnings_df['earnings_date_90d'] = pd.to_datetime(earnings_df['earnings_date']) - timedelta(days=90)

    # Merge with trades and forward fill dates
    trades = pd.merge(trades, earnings_df, how='left', left_on=['symbol', 'just_date'], right_on=['symbol', 'earnings_date_90d'])
    trades['earnings_date'] = trades.groupby(['symbol']).ffill()['earnings_date']
    trades['earnings_date'] = pd.to_datetime(trades['earnings_date'])

    # Calculate difference between date and next earnings date
    trades['earnings_difference'] = trades['just_date'] - trades['earnings_date']
    trades['earnings_difference'] = trades['earnings_difference'].dt.days

    # Add Previous day price column for Trailing Loss calculation
    #trades['previous_day_price'] = trades.groupby(['symbol']).shift(1)['close_price_x']

    # Drop Columns
    trades.drop(columns=['earnings_date_90d', 'earnings_date'], inplace=True)
