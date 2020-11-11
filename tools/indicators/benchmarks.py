import pandas as pd
import numpy as np
import ta

def benchmark_prices(all_prices, benchmark_path='../docs/benchmarks.csv'):
    """
    Function to create a column with benchmark close price
    """
    # Create list of benchmark symbols
    benchmarks = pd.read_csv('../docs/benchmarks.csv')
    benchmarks = benchmarks[['symbol', 'description']]
    benchmarks_list = [list(row) for row in benchmarks.values]

    # For each benchmark on list, create a new column, then we'll have a dataframe with dates and close prices in different columns
    for idx, benchmark in enumerate(benchmarks_list):
        if benchmark[0] == 'SPY':
            main_df = all_prices[all_prices['symbol'] == benchmark[0]][['timestamp', 'close_price']]
            column_name = benchmark[1] + '_close_price'
            main_df.columns = ['timestamp', column_name]

    # List of benchmark columns
    benchmark_columns = list(main_df.columns)[1:]

    # Create some metrics
    for benchmark in benchmark_columns:

        # MACD Histogram
        #macd = ta.trend.MACD(close=main_df[benchmark], n_slow=65, n_fast=30, n_sign=22)
        #main_df[f'macd_hist_{benchmark}'] = macd.macd_diff()

        for x in [1, 2, 3, 10, 20, 30, 60, 90, 120, 150, 180]:

            # Define EMA
            #ema = ta.trend.EMAIndicator(close=main_df[benchmark], n=x)
            #main_df[f'sma_{x}_{benchmark}'] = ema.ema_indicator()
            #main_df[f'sma_{x}_{benchmark}_shift20'] = main_df[f'sma_{x}_{benchmark}'].shift(20)
            #main_df[f'sma_{x}_{benchmark}_shift60'] = main_df[f'sma_{x}_{benchmark}'].shift(60)

            # Return
            main_df[f'{x}d_return_{benchmark}'] = main_df[benchmark].pct_change(x) + 1

            #for i in [10, 20, 30, 60, 90, 120, 150, 180]:
            #    main_df[f'{benchmark}_{i}d_moving_return'] = main_df[f'daily_return_{benchmark}'].rolling(window=i).apply(np.prod, raw=True)

    # Merge it back with all prices
    all_prices = pd.merge(all_prices, main_df, on='timestamp', how='left')

    return all_prices, benchmark_columns

