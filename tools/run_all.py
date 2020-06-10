import pandas as pd
import time
from os import path

from extract.price_extract import main_prices
from extract.zacks_extract import main_ratio
from extract.market_cap import main_market_cap
from indicators.indicators import main_indicators
from signals.create_signals import main_signals

def main(full_refresh=False,
        pe_full_refresh=False,
        number_of_signals=5,
        skip_price=False,
        skip_indicators=False,
        skip_ratios=False,
        stocks_path='../docs/my_stocks.csv'):

    # Start Tracking Time
    start = time.time()

    """
    UPDATE PRICES
    """

    if skip_price == False:

        # Extract Prices using stocks in csv file
        all_prices, my_stocks_symbols = main_prices(full_refresh=full_refresh, do_not_refresh=False, stocks_path=stocks_path)

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
    print('')

    # Start Tracking Time 2
    start2 = time.time()

    """
    PE/PB RATIO & MARKET CAP
    """
    print('Calculate PE/PB Ratios and Market Cap')

    if skip_ratios == False:
        if pe_full_refresh == False:
            pe_and_prices = main_ratio(all_prices, 'pe_ratio', full_refresh=False)
            pb_and_prices = main_ratio(pe_and_prices, 'price_to_book_value', full_refresh=False)
        else:
            print('Running Full Refresh of PE/PB Ratios')
            pe_and_prices = main_ratio(all_prices, 'pe_ratio', full_refresh=True)
            pb_and_prices = main_ratio(pe_and_prices, 'price_to_book_value', full_refresh=True)

        # Calculate Market Cap and Calculate Weighted Returns
        ratios_and_prices = main_market_cap(pb_and_prices)

    else:
        print('Skipping process. No refresh.')

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2)) + ' seconds to update PE/PB Ratios and Market Caps.')
    print('')

    # Start Tracking Time 2
    start2 = time.time()
    """
    RUN INDICATORS
    """

    if skip_indicators == False:
        # Create all indicators
        indicators = main_indicators(ratios_and_prices, my_stocks_symbols, full_refresh=True)
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
            indicators = main_indicators(ratios_and_prices, my_stocks_symbols, full_refresh=True)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2) / 60) + ' minutes to generate indicators.')
    print('')

    # Start Tracking Time 2
    start2 = time.time()

    """
    CREATE BUY/SELL SIGNALS
    """

    # Create Trades/Buy and Sell Signals
    trades = main_signals(indicators, number_of_signals=number_of_signals)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2)) + ' seconds to generate Buy and Sell Signals')
    print('')
    print('The whole process took ' + str(int(end - start) / 60) + ' minutes. Happy Trading! =)')

    return trades
