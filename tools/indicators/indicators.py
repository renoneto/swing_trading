from datetime import datetime as dt, timedelta
import multiprocessing.dummy as mp
from multiprocessing import Pool, Manager, cpu_count
import warnings
import time
import os.path
from os import path

from statistics import median, mean, stdev

import numpy as np
import pandas as pd
import ta

warnings.filterwarnings('ignore')


def run_indicators(df
                   , results_list
                   , symbol
                   , NaN = np.nan
                   , min_max_rows = [5, 10, 15, 20, 25, 30, 60, 90, 120]):
    """
    Create all indicators based on prices.
    This function takes a dataframe with prices, a symbol and a list to store the results.
    """

    # RSI Bins
    bins = list(range(0,101,10))
    labels = list(range(10))

    # Add ta features filling NaN values
    df_2 = df[df['symbol'] == symbol]

    # Add empty Columns
    df_2['low_is_min_7'] = NaN
    df_2['low_is_min_14'] = NaN

    # Create empty_df
    empty_df = df_2.copy()
    empty_df['momentum_rsi'] = 0
    empty_df['index'] = 0
    empty_df = empty_df[empty_df['symbol'] == 'Nothing Here']

    # Check for number of days
    length = len(df_2)

    if length > 14:
        # Close price shift
        df_2['close_price_shift'] = df_2['close_price'].shift(-1)

         # Shift prices one day
        df_2[['high_previous', 'low_previous', 'close_previous', 'open_previous']] = df_2[['high_price', 'low_price', 'close_price', 'open_price']].shift(1)

        # Support and Resistance Prices
        #for row in min_max_rows:
            # Calculations
        #    df_2[f'low_min_d{row}'] = df_2['low_price'].rolling(row).min()
        #    df_2[f'low_min_d{row}_shift1_{row}'] = df_2[f'low_min_d{row}'].shift(row)
        #    df_2[f'high_max_d{row}'] = df_2['high_price'].rolling(row).max()
        #    df_2[f'high_max_d{row}_shift1_{row}'] = df_2[f'high_max_d{row}'].shift(row)
        #    df_2[f'close_min_d{row}'] = df_2['close_price'].rolling(row).min()
        #    df_2[f'open_min_d{row}'] = df_2['open_price'].rolling(row).min()
        #    df_2[f'close_max_d{row}'] = df_2['close_price'].rolling(row).max()
        #    df_2[f'open_max_d{row}'] = df_2['open_price'].rolling(row).max()

        # Year from timestamp
        df_2['year'] = pd.DatetimeIndex(df_2['timestamp']).year

        # Shift Price
        df_2['close_price_shift'] = df_2['close_price'].shift(1)
        df_2['daily_return'] = (df_2['close_price'] / df_2['close_price_shift'])

        # Calculate Past Returns
        for i in [1, 2, 3, 4, 5, 7]:
            df_2[f'moving_{i}d_return'] = (df_2['daily_return']).rolling(window=i).apply(np.prod, raw=True)

        # Calculate min daily moves
        for i in [30, 60, 90]:
            df_2[f'moving_{i}d_return_mean'] = df_2['daily_return'].rolling(window=i).mean()
            df_2[f'moving_{i}d_return_std'] = df_2['daily_return'].rolling(window=i).std()
            df_2[f'moving_{i}d_min_return'] = df_2[f'moving_{i}d_return_mean'] - (2 * df_2[f'moving_{i}d_return_mean'])

        # Simple Moving Average and Stationary Moving Average
        for i in [10, 20, 50, 100, 200]:

            df_2[f'sma_{i}d'] = df_2['close_price'].rolling(window=i).mean()
            df_2[f'sma_{i}d_shift'] = df_2[f'sma_{i}d'].shift(1)
            df_2[f'sma_{i}d_coef'] = (-df_2[f'sma_{i}d'].shift(i) + df_2[f'sma_{i}d']) / 2
            df_2[f'sma_{i}d_std'] = df_2['close_price'].rolling(window=i).std()
            df_2[f'stationary_sma_{i}d'] = df_2['close_price'] - df_2[f'sma_{i}d']
            df_2[f'stationary_sma_{i}d_zscore'] = df_2[f'stationary_sma_{i}d'] / df_2[f'sma_{i}d_std']
            df_2[f'stationary_sma_{i}d_zscore_shift'] = df_2[f'stationary_sma_{i}d_zscore'].shift(5)
            df_2[f'stationary_sma_{i}d_zscore_shift_2'] = df_2[f'stationary_sma_{i}d_zscore'].shift(10)
            df_2[f'stationary_sma_{i}d_zscore_min'] = df_2[f'stationary_sma_{i}d_zscore'].rolling(window=i).min()
            df_2[f'stationary_sma_{i}d_zscore_max'] = df_2[f'stationary_sma_{i}d_zscore'].rolling(window=i).max()

        for i in [10, 20, 100, 200]:

            y = int(i/2)
            y2 = int(1.5 * i)

            df_2[f'stationary_sma_{i}d_zscore_min_shift_1'] = df_2[f'stationary_sma_{i}d_zscore_min'].shift(y)
            df_2[f'stationary_sma_{i}d_zscore_min_shift_2'] = df_2[f'stationary_sma_{i}d_zscore_min'].shift(i)
            df_2[f'stationary_sma_{i}d_zscore_min_shift_3'] = df_2[f'stationary_sma_{i}d_zscore_min'].shift(y2)
            df_2[f'stationary_sma_{i}d_zscore_min_all'] = df_2[[f'stationary_sma_{i}d_zscore_min_shift_1', f'stationary_sma_{i}d_zscore_min_shift_2', f'stationary_sma_{i}d_zscore_min_shift_3']].min(axis=1)
            df_2[f'stationary_sma_{i}d_zscore_min_all_shift'] = df_2[f'stationary_sma_{i}d_zscore_min_all'].shift(5)
            df_2[f'stationary_sma_{i}d_zscore_min_all_shift_2'] = df_2[f'stationary_sma_{i}d_zscore_min_all'].shift(10)

        for i in [10, 20, 50, 100, 200]:
            for y in [20, 50, 100, 200]:
                if i < y:
                    df_2[f'sma_{i}d_{y}d_ratio'] = df_2[f'sma_{i}d'] / df_2[f'sma_{y}d']
                    df_2[f'sma_{i}d_{y}d_ratio_shift'] = df_2[f'sma_{i}d_{y}d_ratio'].shift(1)
                    df_2[f'sma_{i}d_{y}d_ratio_shift_2'] = df_2[f'sma_{i}d_{y}d_ratio'].shift(2)
                    df_2[f'sma_{i}d_{y}d_ratio_shift_3'] = df_2[f'sma_{i}d_{y}d_ratio'].shift(3)
                    df_2[f'sma_{i}d_{y}d_ratio_coef_2d'] = (df_2[f'sma_{i}d_{y}d_ratio'] - df_2[f'sma_{i}d_{y}d_ratio'].shift(1)) / 2
                    df_2[f'sma_{i}d_{y}d_ratio_avg'] = df_2[f'sma_{i}d_{y}d_ratio'].rolling(window=200).mean()
                    df_2[f'sma_{i}d_{y}d_ratio_std'] = df_2[f'sma_{i}d_{y}d_ratio'].rolling(window=200).std()
                    for z, w in [(2,2), (1.5, 15), (1,1)]:
                        df_2[f'sma_{i}d_{y}d_ratio_{w}std_up'] = df_2[f'sma_{i}d_{y}d_ratio_avg'] + (z * df_2[f'sma_{i}d_{y}d_ratio_std'])
                        df_2[f'sma_{i}d_{y}d_ratio_{w}std_down'] = df_2[f'sma_{i}d_{y}d_ratio_avg'] - (z * df_2[f'sma_{i}d_{y}d_ratio_std'])
                        df_2[f'sma_{i}d_{y}d_ratio_{w}std_up_diff'] = df_2[f'sma_{i}d_{y}d_ratio_{w}std_up'] - df_2[f'sma_{i}d_{y}d_ratio']
                        df_2[f'sma_{i}d_{y}d_ratio_{w}std_down_diff'] =  df_2[f'sma_{i}d_{y}d_ratio'] - df_2[f'sma_{i}d_{y}d_ratio_{w}std_down']

        # Get RSI
        df_2['momentum_rsi'] = ta.momentum.RSIIndicator(close=df_2['close_price'], n=14).rsi()
        df_2['momentum_rsi_low'] = ta.momentum.RSIIndicator(close=df_2['low_price'], n=14).rsi()
        df_2['momentum_rsi_high'] = ta.momentum.RSIIndicator(close=df_2['high_price'], n=14).rsi()

        for i in ['', '_low', '_high']:
            # Create bins of rsi and label them
            #df_2[f'rsi_bins{i}'] = pd.cut(df_2[f'momentum_rsi{i}'], bins=bins, labels=labels)
            #df_2[f'rsi_bins{i}'] = pd.to_numeric(df_2[f'rsi_bins{i}'], errors='coerce')
            df_2[f'rsi_bins{i}'] = pd.to_numeric(df_2[f'momentum_rsi{i}'], errors='coerce')

        for y in [1, 2, 3]:
            # Create a shift of the bin, to compare current with the previous
            df_2[f'rsi_bins_shift_{y}d'] = df_2['rsi_bins'].shift(y).fillna(0)
            df_2[f'rsi_bins_shift_{y}d_low'] = df_2['rsi_bins_low'].shift(y).fillna(0)
            df_2[f'rsi_bins_shift_{y}d_high'] = df_2['rsi_bins_high'].shift(y).fillna(0)

        for i in [7, 14, 21, 28, 35, 70, 105, 140, 175, 210]:
            # Get min rsi
            df_2[f'rsi_{i}'] = df_2['momentum_rsi'].rolling(i).min()

        for i in [35, 70, 105, 140, 175, 210]:
            # Get rsi std
            df_2[f'rsi_std_{i}'] = df_2['rsi_bins'].rolling(i).std()

            # Get min rsi
            df_2[f'rsi_{i}_min_bin'] = df_2['rsi_bins'].rolling(i).min()
            df_2[f'rsi_{i}_min_bin_low'] = df_2['rsi_bins_low'].rolling(i).min()
            df_2[f'rsi_{i}_min_bin_high'] = df_2['rsi_bins_high'].rolling(i).min()

        for i in ['', '_low', '_high']:
            # Get Min rsi shift
            df_2[f'rsi_35_min_bin_shift_1d{i}'] = df_2[f'rsi_35_min_bin{i}'].shift(1)
            df_2[f'rsi_35_min_bin_shift_2d{i}'] = df_2[f'rsi_35_min_bin{i}'].shift(2)

        # Get shifted min rsi
        for i in ['', '_low', '_high']:
            df_2[f'rsi_35_min_bin_shifted_1{i}'] = df_2[f'rsi_35_min_bin{i}'].shift(35)
            df_2[f'rsi_35_min_bin_shifted_2{i}'] = df_2[f'rsi_35_min_bin{i}'].shift(70)
            df_2[f'rsi_35_min_bin_shifted_3{i}'] = df_2[f'rsi_35_min_bin{i}'].shift(105)

        df_2['rsi_70_min_bin_shifted_1'] = df_2['rsi_70_min_bin'].shift(70)
        df_2['rsi_70_min_bin_shifted_2'] = df_2['rsi_70_min_bin'].shift(140)
        df_2['rsi_70_min_bin_shifted_3'] = df_2['rsi_70_min_bin'].shift(210)

        # Get max rsi
        for i in [35, 70, 105, 140, 175, 210]:
            df_2[f'rsi_{i}_max_bin'] = df_2['rsi_bins'].rolling(i).max()
            df_2[f'rsi_{i}_max_bin_high'] = df_2['rsi_bins_high'].rolling(i).max()

        # Median min rsi
        df_2['rsi_median_min_2'] = (df_2['rsi_35_min_bin'] + df_2['rsi_70_min_bin']) / 2
        df_2['rsi_median_min_4'] = (df_2['rsi_35_min_bin'] + df_2['rsi_70_min_bin'] +
                                     df_2['rsi_105_min_bin'] + df_2['rsi_140_min_bin']) / 4
        df_2['rsi_median_min_6'] = (df_2['rsi_35_min_bin'] + df_2['rsi_70_min_bin'] +
                                     df_2['rsi_105_min_bin'] + df_2['rsi_140_min_bin'] +
                                     df_2['rsi_175_min_bin'] + df_2['rsi_210_min_bin']) / 6

        # Difference Median RSI and RSI
        for i in [2, 4, 6]:
            df_2[f'rsi_median_min_{i}_diff'] = df_2['rsi_bins'] - df_2[f'rsi_median_min_{i}']

        # Rsi Signals
        for i in [1, 2, 3]:
            df_2[f'rsi_signal_start_35_{i}'] = np.where((df_2['rsi_bins'] == df_2[f'rsi_35_min_bin_shifted_{i}']), 1, 0)
            df_2[f'rsi_signal_start_70_{i}'] = np.where((df_2['rsi_bins'] == df_2[f'rsi_70_min_bin_shifted_{i}']), 1, 0)

        # Sum of signals
        df_2['sum_rsi_signal_start'] = (df_2['rsi_signal_start_35_1'] + df_2['rsi_signal_start_35_2'] +
                                        df_2['rsi_signal_start_35_3'] + df_2['rsi_signal_start_70_1'] +
                                        df_2['rsi_signal_start_70_2'] + df_2['rsi_signal_start_70_3'] )

        # MACD
        macd = ta.trend.MACD(close=df_2['close_price'])
        df_2['macd_line'] = macd.macd()
        df_2['macd_hist'] = macd.macd_diff()
        df_2['macd_signal_line'] = macd.macd_signal()
        df_2['macd_hist_1d_shift'] = df_2['macd_hist'].shift(1)
        df_2['macd_hist_2d_shift'] = df_2['macd_hist'].shift(2)
        df_2['macd_hist_3d_shift'] = df_2['macd_hist'].shift(3)

        # Shift line
        df_2['macd_line_shift_1d'] = df_2['macd_line'].shift(1)
        df_2['macd_line_shift_2d'] = df_2['macd_line'].shift(2)
        df_2['macd_line_shift_3d'] = df_2['macd_line'].shift(3)
        df_2['macd_sig_line_shift_1d'] = df_2['macd_signal_line'].shift(1)
        df_2['macd_sig_line_shift_2d'] = df_2['macd_signal_line'].shift(2)
        df_2['macd_sig_line_shift_3d'] = df_2['macd_signal_line'].shift(3)

        # MACD - mins
        df_2['macd_hist_2d_min'] = df_2['macd_hist'].rolling(2).min()
        df_2['macd_hist_3d_min'] = df_2['macd_hist'].rolling(3).min()
        df_2['macd_hist_5d_min'] = df_2['macd_hist'].rolling(5).min()
        df_2['macd_hist_7d_min'] = df_2['macd_hist'].rolling(7).min()
        df_2['macd_hist_35d_min'] = df_2['macd_hist'].rolling(35).min()
        df_2['macd_hist_70d_min'] = df_2['macd_hist'].rolling(70).min()
        df_2['macd_hist_140d_min'] = df_2['macd_hist'].rolling(140).min()

        # MACD - min shifts
        df_2['macd_hist_70d_min_shift_1'] = df_2['macd_hist_70d_min'].shift(70)
        df_2['macd_hist_70d_min_shift_2'] = df_2['macd_hist_70d_min'].shift(140)

        # MACD - shift
        df_2['macd_hist_3d_min_shift'] = df_2['macd_hist_3d_min'].shift(1)
        df_2['macd_hist_5d_min_shift'] = df_2['macd_hist_5d_min'].shift(1)
        df_2['macd_hist_7d_min_shift'] = df_2['macd_hist_7d_min'].shift(1)

        # MACD - min - shift
        df_2['macd_hist_3d_min_diff'] = df_2['macd_hist_3d_min'] - df_2['macd_hist_3d_min_shift']
        df_2['macd_hist_5d_min_diff'] = df_2['macd_hist_5d_min'] - df_2['macd_hist_5d_min_shift']
        df_2['macd_hist_7d_min_diff'] = df_2['macd_hist_7d_min'] - df_2['macd_hist_7d_min_shift']

        # MACD - rolling std - avg
        df_2['macd_hist_std_35'] = df_2['macd_hist'].rolling(35).std()
        df_2['macd_hist_std_70'] = df_2['macd_hist'].rolling(70).std()
        df_2['macd_hist_std_140'] = df_2['macd_hist'].rolling(140).std()
        df_2['macd_hist_avg_35'] = df_2['macd_hist'].rolling(35).mean()
        df_2['macd_hist_avg_70'] = df_2['macd_hist'].rolling(70).mean()
        df_2['macd_hist_avg_140'] = df_2['macd_hist'].rolling(140).mean()

        # MACD - Growth
        df_2['macd_hist_3d_min_coef'] = (-df_2['macd_hist'].shift(3) + df_2['macd_hist'].shift(1)) / 2
        df_2['macd_hist_5d_min_coef'] = (-df_2['macd_hist'].shift(5) + df_2['macd_hist'].shift(1)) / 4
        df_2['macd_hist_7d_min_coef'] = (-df_2['macd_hist'].shift(7) + df_2['macd_hist'].shift(1)) / 6

        # MACD - Growth - Line
        df_2['macd_line_3d_min_coef'] = (-df_2['macd_line'].shift(3) + df_2['macd_line'].shift(1)) / 2
        df_2['macd_line_5d_min_coef'] = (-df_2['macd_line'].shift(5) + df_2['macd_line'].shift(1)) / 4
        df_2['macd_line_7d_min_coef'] = (-df_2['macd_line'].shift(7) + df_2['macd_line'].shift(1)) / 6

        # MACD - Growth - shift
        df_2['macd_hist_3d_min_coef_shift_1'] = df_2['macd_hist_3d_min_coef'].shift(1)
        df_2['macd_hist_3d_min_coef_shift_2'] = df_2['macd_hist_3d_min_coef'].shift(2)
        df_2['macd_hist_3d_min_coef_shift_3'] = df_2['macd_hist_3d_min_coef'].shift(3)

        # MACD - Growth - shift - Line
        df_2['macd_line_3d_min_coef_shift_1'] = df_2['macd_line_3d_min_coef'].shift(1)
        df_2['macd_line_3d_min_coef_shift_2'] = df_2['macd_line_3d_min_coef'].shift(2)
        df_2['macd_line_3d_min_coef_shift_3'] = df_2['macd_line_3d_min_coef'].shift(3)

        # Initialize Bollinger Bands Indicator
        indicator_bb = ta.volatility.BollingerBands(close=df_2["close_price"], n=14, ndev=2)

        # Add Bollinger Bands features
        df_2['bb_bbm'] = indicator_bb.bollinger_mavg()
        df_2['bb_bbh'] = indicator_bb.bollinger_hband()
        df_2['bb_bbl'] = indicator_bb.bollinger_lband()
        df_2['bb_std'] = (df_2['bb_bbh'] - df_2['bb_bbm']) / 2
        df_2['bb_std_avg_100'] = df_2['bb_std'].rolling(100).mean()
        df_2['bb_bbl_diff_std'] = (df_2['close_price'] - df_2['bb_bbl']) / df_2['bb_std']
        df_2['bb_bbh_diff_std'] = (df_2['close_price'] - df_2['bb_bbh']) / df_2['bb_std']

        # SMA
        sma = ta.momentum.AwesomeOscillatorIndicator(high=df_2['high_price'], low=df_2['low_price'])
        df_2['sma_oscillator'] = sma.ao()

        # Remove first 14 days
        df_2 = df_2.reset_index(drop=True)
        df_2 = df_2.reset_index()
        df_2 = df_2[df_2['index'] > 14]

        # Future Returns
        future = df_2[['timestamp', 'close_price']]
        future['timestamp'] = pd.to_datetime(future['timestamp'])
        future.sort_values(by=['timestamp'], ascending=False, inplace = True)
        future['next_day_price'] = future['close_price'].shift(1)
        future['next_day_return'] = (future['next_day_price'] / future['close_price'])
        future['next_3d_return'] = (future['next_day_return']).rolling(window=3).apply(np.prod, raw=True)
        future['next_5d_return'] = (future['next_day_return']).rolling(window=5).apply(np.prod, raw=True)
        future['next_7d_return'] = (future['next_day_return']).rolling(window=7).apply(np.prod, raw=True)
        future['next_14d_return'] = (future['next_day_return']).rolling(window=14).apply(np.prod, raw=True)
        future['next_21d_return'] = (future['next_day_return']).rolling(window=21).apply(np.prod, raw=True)

        # Merge output with future returns
        df_2 = pd.merge(df_2, future, on='timestamp', how='left')

        return results_list.append(df_2)

    else:

        # If there's not enough data, it will output an empty dataframe
        return results_list.append(empty_df)

def run_indicators_list(stocks_list, results_list, stock_prices):
    """
    Function to run indicators in parallel
    """

    print('Running indicators ...')
    total = len(stocks_list)

    for idx, stock in enumerate(stocks_list):
        run_indicators(stock_prices, results_list, stock)

def indicators_parallel(all_prices, my_stocks_symbols):
    """
    Main function to run indicators in parallel
    """

    # Create Pool with Processes and Manager
    pool = Pool(processes=cpu_count() - 1)
    manager = Manager()

    # Create list to store results
    results_list = manager.list()

    # Start running Pools
    [pool.apply_async(run_indicators_list, args=(stocks_chunk, results_list, all_prices)) for stocks_chunk in my_stocks_symbols]
    pool.close()
    pool.join()

    # Create dataframe with results
    indicators_df = pd.concat(results_list)

    return indicators_df

def main_indicators(all_prices, my_stocks_symbols, full_refresh=True):

    # Check if file exists
    existing_file = path.exists('../output/all_indicators.feather')

    if full_refresh == False and existing_file == True:

        print('Running Incremental Update of Indicators')

        # Calculate Start date of Prices and incremental date update
        start_date = dt.today() - timedelta(days=450)
        incremental_date = dt.today() - timedelta(days=31)

        # Open existing indicators
        indicators_df = pd.read_feather('../output/all_indicators.feather')
        indicators_df = indicators_df[pd.to_datetime(indicators_df['timestamp']) <= incremental_date]

        # Keep only incremental prices
        incremental_prices = all_prices[pd.to_datetime(all_prices['timestamp']) >= start_date]

        # Run Indicators
        incremental_indicators = indicators_parallel(incremental_prices, my_stocks_symbols=my_stocks_symbols)
        incremental_indicators = incremental_indicators[pd.to_datetime(incremental_indicators['timestamp']) > incremental_date]

        # Open existing Indicators
        indicators_df = pd.concat([indicators_df, incremental_indicators])

        return indicators_df

    else:

        if full_refresh == False and existing_file == False:
            print('The file does not exist yet. Running a Full Refresh')
        else:
            print('Running Full Refresh of Indicators. This will take some time.')

        # Run main indicators without any filters
        indicators_df = indicators_parallel(all_prices, my_stocks_symbols=my_stocks_symbols)

        print('Exporting Indicators')

        # Export indiciators
        indicators_df['just_date'] = indicators_df['just_date'].astype(str)
        indicators_df.reset_index(drop=True).to_feather('../output/all_indicators.feather')

        return indicators_df






