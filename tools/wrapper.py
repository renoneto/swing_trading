import pandas as pd
import time
from os import path

from extract.price_extract import main_prices
from extract.fundamentals import main_fundamentals
from extract.market_cap import main_market_cap
from indicators.indicators import main_indicators
from signals.create_signals import main_signals
from indicators.benchmarks import benchmark_prices
from database.postgres import PostgresTable

def main(stocks_table,
        prices_table,
        indicators_table,
        skip_price=False,
        skip_indicators=False):

    # Start Tracking Time
    start = time.time()

    """
    UPDATE PRICES
    """

    if skip_price == False:
        # Download Prices - All-Time
        main_prices(stocks_table, prices_table)

        # End Timer
        end = time.time()
        print(f'{str(round(int(end - start) / 60, 1))} minutes to extract prices.')

    # Start Tracking Time 2
    start2 = time.time()

    """
    RUN INDICATORS & BENCHMARKS
    """

    if skip_indicators == False:
        # Create all indicators
        main_indicators(prices_table, stocks_table, indicators_table)

        # End Timer
        end = time.time()
        print(f'{str(round(int(end - start2) / 60, 1))} minutes to generate indicators.')

    # # Start Tracking Time 2
    # start2 = time.time()

    # """
    # UPDATE FUNDAMENTALS
    # """
    # print('Add Fundamentals')

    # fundamental_prices = main_fundamentals(all_prices, indicators)

    # # End Timer
    # end = time.time()
    # print('It took ' + str(int(end - start2) / 60) + ' minutes to update fundamentals.')
    # print('')

    # # Start Tracking Time 2
    # start2 = time.time()
    # """
    # CREATE BUY/SELL SIGNALS
    # """

    # # Create Trades/Buy and Sell Signals
    # trades = main_signals(fundamental_prices, number_of_signals=number_of_signals)

    # # End Timer
    end = time.time()
    # print('It took ' + str(int(end - start2) / 60) + ' minutes to generate Buy and Sell Signals')
    # print('')
    print(f'The whole process took {str(round(int(end - start) / 60, 1))} minutes. Happy Trading! =)')

if __name__ == '__main__':

    stocks_table = PostgresTable('postgresql://localhost/swing_trading', 'raw_finviz_pull')
    prices_table = PostgresTable('postgresql://localhost/swing_trading', 'raw_prices')
    indicators_table = PostgresTable('postgresql://localhost/swing_trading', 'indicators')
    main(stocks_table, prices_table, indicators_table, skip_price=True)
