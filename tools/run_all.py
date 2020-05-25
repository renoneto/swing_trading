import pandas as pd
import time
from os import path

from extract.price_extract import main_prices
from indicators.indicators import main_indicators
from signals.create_signals import main_signals

def main(full_refresh=False,
        number_of_signals=5,
        skip_price=False,
        skip_indicators=False):

    # Start Tracking Time
    start = time.time()

    if skip_price == False:

        # Extract Prices using stocks in csv file
        all_prices, my_stocks_symbols = main_prices(full_refresh=full_refresh, do_not_refresh=False)

    else:

        # Check if file exists
        existing_file = path.exists('../docs/prices.csv')

        if existing_file == True:
            # Extract Prices using stocks in csv file
            all_prices, my_stocks_symbols = main_prices(full_refresh=full_refresh, do_not_refresh=True)

        else:
            print('Price File doesnt exist. Running Full Refresh of Prices')
            # Extract Prices using stocks in csv file
            all_prices, my_stocks_symbols = main_prices(full_refresh=True, do_not_refresh=False)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start)) + ' seconds to extract prices.')

    # Start Tracking Time 2
    start2 = time.time()

    if skip_indicators == False:
        # Create all indicators
        indicators = main_indicators(all_prices, my_stocks_symbols, full_refresh=True)
    else:

        # Check if file exists
        existing_file = path.exists('../output/all_indicators.feather')

        if existing_file == True:

            print('Reading indicators file')
            # Read file
            indicators = pd.read_feather('../output/all_indicators.feather')

        else:
            print('Cannot skip indicators because file doesnt exist. Running Full Refresh.')
            # Create all indicators
            indicators = main_indicators(all_prices, my_stocks_symbols, full_refresh=True)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2) / 60) + ' minutes to generate indicators.')

    # Start Tracking Time 2
    start2 = time.time()

    # Create Trades/Buy and Sell Signals
    trades = main_signals(indicators, number_of_signals=number_of_signals)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2)) + ' seconds to generate Buy and Sell Signals')
    print('The whole process took ' + str(int(end - start) / 60) + ' minutes.')

    return trades
