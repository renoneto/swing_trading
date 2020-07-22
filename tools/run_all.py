import pandas as pd
import time
from os import path

from extract.price_extract import main_prices
from extract.fundamentals import main_fundamentals
from extract.market_cap import main_market_cap
from indicators.indicators import main_indicators
from signals.create_signals import main_signals
from indicators.benchmarks import benchmark_prices

def main(full_refresh=False,
        number_of_signals=7,
        skip_price=False,
        skip_indicators=False,
        export_indicators=True,
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
    RUN INDICATORS & BENCHMARKS
    """

    # Create Benchmark columns before metrics
    benchmark_and_prices, benchmarks_columns = benchmark_prices(all_prices)

    if skip_indicators == False:
        # Create all indicators
        indicators = main_indicators(benchmark_and_prices, my_stocks_symbols, benchmarks_columns, export=export_indicators)
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
            indicators = main_indicators(benchmark_and_prices, my_stocks_symbols, benchmarks_columns, export=export_indicators)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2) / 60) + ' minutes to generate indicators.')
    print('')

    # Start Tracking Time 2
    start2 = time.time()

    """
    UPDATE FUNDAMENTALS
    """
    print('Add Fundamentals')

    fundamental_prices = main_fundamentals(all_prices, indicators)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2) / 60) + ' minutes to update fundamentals.')
    print('')

    # Start Tracking Time 2
    start2 = time.time()
    """
    CREATE BUY/SELL SIGNALS
    """

    # Create Trades/Buy and Sell Signals
    trades = main_signals(fundamental_prices, number_of_signals=number_of_signals)

    # End Timer
    end = time.time()
    print('It took ' + str(int(end - start2) / 60) + ' minutes to generate Buy and Sell Signals')
    print('')
    print('The whole process took ' + str(int(end - start) / 60) + ' minutes. Happy Trading! =)')

    return trades
