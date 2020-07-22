import pandas as pd
import numpy as np

def benchmark_prices(all_prices, benchmark_path='../docs/benchmarks.csv'):
    """
    Function to create a column with benchmark close price
    """
    # Create list of benchmark symbols
    benchmarks = pd.read_csv('../docs/benchmarks.csv')
    benchmarks_list = list(benchmarks['symbol'])

    # For each benchmark on list, create a new column, then we'll have a dataframe with dates and close prices in different columns
    for idx, benchmark in enumerate(benchmarks_list):
        if idx == 0:
            main_df = all_prices[all_prices['symbol'] == benchmark][['timestamp', 'close_price']]
            column_name = benchmark + '_close_price'
            main_df.columns = ['timestamp', column_name]
        else:
            temp_df = all_prices[all_prices['symbol'] == benchmark][['timestamp', 'close_price']]
            column_name = benchmark + '_close_price'
            temp_df.columns = ['timestamp', column_name]
            main_df = pd.merge(main_df, temp_df, how='left', on='timestamp')

    # List of benchmark columns

    benchmark_columns = list(main_df.columns)[1:]

    # Create some metrics
    for benchmark in benchmark_columns:
        main_df[f'daily_return_{benchmark}'] = main_df[benchmark].pct_change() + 1

        for x in [20, 50, 100, 200]:
            main_df[f'sma_{x}_{benchmark}'] = main_df[benchmark].rolling(x).mean()
            main_df[f'sma_{x}_{benchmark}_shift1'] = main_df[f'sma_{x}_{benchmark}'].shift(1)
            main_df[f'sma_{x}_{benchmark}_shift3'] = main_df[f'sma_{x}_{benchmark}'].shift(3)
            main_df[f'sma_{x}_{benchmark}_shift5'] = main_df[f'sma_{x}_{benchmark}'].shift(5)
            main_df[f'sma_{x}_{benchmark}_shift10'] = main_df[f'sma_{x}_{benchmark}'].shift(10)
            main_df[f'sma_{x}_{benchmark}_shift20'] = main_df[f'sma_{x}_{benchmark}'].shift(20)

            for i in [10, 20, 30, 60, 90, 180]:
                main_df[f'{benchmark}_{i}d_moving_return'] = main_df[f'daily_return_{benchmark}'].rolling(window=i).apply(np.prod, raw=True)

    # Merge it back with all prices
    all_prices = pd.merge(all_prices, main_df, on='timestamp', how='left')

    return all_prices, benchmark_columns

