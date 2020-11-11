from datetime import datetime
import functools
import os
import time
import sys

from schedule import run_all
# to import local fuctions
sys.path.insert(0, '../tools')

import schedule

from extract.earnings import extract_earnings
from extract.fundamentals import update_fundamental_dates, extract_fundamentals
from extract.price_extract import main_prices
from run_all import main
from communicate.discord import send_message_to_discord

from extract.fundamentals import main_fundamentals
from extract.market_cap import main_market_cap
from indicators.indicators import main_indicators
from signals.create_signals import main_signals
from indicators.benchmarks import benchmark_prices

# This decorator can be applied to
def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print('LOG: Running job "%s"' % func.__name__)
        result = func(*args, **kwargs)
        print('LOG: Job "%s" completed' % func.__name__)
        return result
    return wrapper

# Check if file date modified equals to today's date
def check_download(path):
    time = os.path.getmtime(path)
    file_date = datetime.fromtimestamp(time).date()
    if file_date == datetime.today().date():
        return True
    else:
        return False

# Check if it's a weekday
def check_weekday():
    # Define variables
    hour = datetime.today().hour
    weekday = datetime.today().weekday()

    # If between 6AM and 2PM and it's a weekday return True
    if weekday < 5:
        return True
    # For the weekend
    elif (weekday > 4) & (datetime.today().hour < 9):
        return True

@with_logging
def run_prices():
    if check_weekday() == True:
        main(full_refresh=True)
        send_message_to_discord('buy_signal?7')

@with_logging
def earnings():
    if check_download("/Users/renovieira/Desktop/swing_trading/docs/earnings.csv") == False:
        extract_earnings()

@with_logging
def fundamentals():
    print('Extracting Fundamentals')
    extract_fundamentals()
    print('Update Fundamental Dates')
    update_fundamental_dates()

# Update Prices every three hours on weekdays
schedule.every().day.at("06:30").do(run_prices)
schedule.every().day.at("09:30").do(run_prices)
schedule.every().day.at("12:00").do(run_prices)
schedule.every().day.at("13:30").do(run_prices)

# Run Earnings and Fundamentals
schedule.every().thursday.at('21:00').do(earnings)
schedule.every().friday.at('23:00').do(fundamentals)

while 1:
    schedule.run_pending()
    time.sleep(1)
