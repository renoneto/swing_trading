import pandas as pd
import time
from os import path
import contextlib

from extract.price_extract import main_prices
from extract.fundamentals import main_fundamentals
from extract.market_cap import main_market_cap
from indicators.indicators import main_indicators
from signals.create_signals import main_signals
from indicators.benchmarks import benchmark_prices


# Add a decorator that will make timer() a context manager
@contextlib.contextmanager
def timer():
    """
    Time the execution of a context block.

        Yields:
        None
    """
    start = time.time()
    # Send control back to the context block
    yield
    end = time.time()

    if end - start < 60:
        print('Elapsed: {:.2f}s'.format(end - start))
    else:
        print('Elapsed: {:.2f} mins'.format((end - start) / 60))
    print('')

def main(full_refresh=False,
        number_of_signals=7,
        skip_price=False,
        skip_indicators=False,
        skip_fundamentals=False,
        export_indicators=True,
        stocks_path='../docs/my_stocks.feather'):
    """
    Function to run prices, create indicators/benchmarks, add fundamentals and generate signals.

        Parameters:
            full_refresh (bool): Indicates whether it will download all historical prices.
            number_of_signals (int): No. of pair signals (buy/sell) that will be analyzed.
            skip_price (bool): If True it will try to read existing price file.
            skip_indicators (bool): If True it will try to read existing indicators file.
            skip_fundamentals (bool): If True it will try to read existing fundamentals file.
            export_indicators (bool): If True it will export indicators file.
            stocks_path (str): Path to stocks file that will be analyzed

        Returns:
            trades (pd.DataFrame): DataFrame with all prices + indicators + columns for each pair signal.
    """

    # Start time to track whole proces
    start = time.time()

    """
    UPDATE PRICES
    """
    with timer():

        if skip_price == False:

            # Extract Prices using stocks in csv file
            all_prices, my_stocks_symbols = main_prices(full_refresh=full_refresh, do_not_refresh=False, stocks_path=stocks_path)

        elif skip_price == True:

            # Check if file exists
            existing_file = path.exists('../docs/prices.feather')

            if existing_file == True:
                # Extract Prices using stocks in csv file
                all_prices, my_stocks_symbols = main_prices(full_refresh=full_refresh, do_not_refresh=True, stocks_path=stocks_path)

            else:
                print('Price File doesnt exist. Running Full Refresh of Prices')
                # Extract Prices using stocks in csv file
                all_prices, my_stocks_symbols = main_prices(full_refresh=True, do_not_refresh=False, stocks_path=stocks_path)

    """
    RUN INDICATORS & BENCHMARKS
    """
    with timer():
        # Create Benchmark columns before metrics
        benchmark_and_prices, benchmarks_columns = benchmark_prices(all_prices)

        if skip_indicators == False:
            # Create all indicators
            indicators = main_indicators(benchmark_and_prices, my_stocks_symbols, benchmarks_columns, export=export_indicators)

        elif skip_indicators == True:

            # Check if file exists
            existing_file = path.exists('../output/all_indicators.feather')

            if existing_file == True:

                print('Reading indicators file')
                # Read file
                indicators = pd.read_feather('../output/all_indicators.feather')

            else:
                print('Cannot skip indicators because file doesnt exist. Running Indicators.')
                # Create all indicators
                indicators = main_indicators(benchmark_and_prices, my_stocks_symbols, benchmarks_columns, export=export_indicators)

    """
    UPDATE FUNDAMENTALS
    """
    print('Add Fundamentals')
    with timer():
        if skip_fundamentals == False:
            fundamental_prices = main_fundamentals(all_prices, indicators)
            fundamental_prices.to_feather('../docs/fundamental_prices.feather')
        else:
            fundamental_prices = pd.read_feather('../docs/fundamental_prices.feather')

    """
    CREATE BUY/SELL SIGNALS
    """
    with timer():
        # Create Trades/Buy and Sell Signals
        trades = main_signals(fundamental_prices, number_of_signals=number_of_signals)

    # End Timer
    end = time.time()
    print('The whole process took ' + str(int(end - start) / 60) + ' minutes. Happy Trading! =)')

    return trades
