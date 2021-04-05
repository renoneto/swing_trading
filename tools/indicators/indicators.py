from multiprocessing import Pool, cpu_count, Manager
import warnings

import numpy as np
import pandas as pd
import ta

from support.custom_functions import chunks

warnings.filterwarnings('ignore')

def run_indicators(price_df, symbol):
    """
    Create all indicators based on prices.
    This function takes a dataframe with prices, a symbol and a list to store the results.
    """
    # RSI Bins
    bins = list(range(0,101,10))
    labels = list(range(10))

    # Add ta features filling NaN values
    indicators_df = price_df[price_df['symbol'] == symbol]

    # Create empty_df
    empty_df = price_df.copy()
    empty_df = empty_df[empty_df['symbol'] == 'Nothing Here']

    # Check for number of days
    length = len(indicators_df)

    if length > 14:
        # Close price shift
        #indicators_df['close_shift'] = indicators_df['close'].shift(-1)

         # Shift prices one day
        #indicators_df[['high_previous', 'low_previous', 'close_previous', 'open_previous']] = indicators_df[['high', 'low', 'close', 'open']].shift(1)

        # Support and Resistance Prices
        #for row in min_max_rows:
            # Calculations
        #    indicators_df[f'low_min_d{row}'] = indicators_df['low'].rolling(row).min()
        #    indicators_df[f'low_min_d{row}_shift1_{row}'] = indicators_df[f'low_min_d{row}'].shift(row)
        #    indicators_df[f'high_max_d{row}'] = indicators_df['high'].rolling(row).max()
        #    indicators_df[f'high_max_d{row}_shift1_{row}'] = indicators_df[f'high_max_d{row}'].shift(row)
        #    indicators_df[f'close_min_d{row}'] = indicators_df['close'].rolling(row).min()
        #    indicators_df[f'open_min_d{row}'] = indicators_df['open'].rolling(row).min()
        #    indicators_df[f'close_max_d{row}'] = indicators_df['close'].rolling(row).max()
        #    indicators_df[f'open_max_d{row}'] = indicators_df['open'].rolling(row).max()

        # Average Volume
        indicators_df['avg_volume'] = indicators_df['volume'].rolling(90).mean()

        # Year and Week No from date
        #indicators_df['year'] = pd.DatetimeIndex(indicators_df['date']).year
        #indicators_df['week_no'] = pd.DatetimeIndex(indicators_df['date']).week
        #indicators_df['weekday_no'] = pd.DatetimeIndex(indicators_df['date']).weekday

        # Shift Price
        for i in [1]: #, 2, 3]:
            indicators_df[f'close_shift_{i}d'] = indicators_df['close'].shift(i)
            indicators_df[f'high_shift_{i}d'] = indicators_df['high'].shift(i)
            indicators_df[f'low_shift_{i}d'] = indicators_df['low'].shift(i)
            indicators_df[f'open_shift_{i}d'] = indicators_df['open'].shift(i)

        # Daily return
        indicators_df['daily_return'] = (indicators_df['close'] / indicators_df['close_shift_1d'])

        # Calculate Past Returns
        for i in [1, 2, 3, 4, 5]: #, 7, 10, 15, 20, 30, 60, 90, 120]:
            indicators_df[f'moving_{i}d_return'] = (indicators_df['daily_return']).rolling(window=i).apply(np.prod, raw=True)

        # Calculate min daily moves
        #for i in [30, 60, 90]:
        #    indicators_df[f'moving_{i}d_return_mean'] = indicators_df['daily_return'].rolling(window=i).mean()
        #    indicators_df[f'moving_{i}d_return_std'] = indicators_df['daily_return'].rolling(window=i).std()
        #    indicators_df[f'moving_{i}d_min_return'] = indicators_df[f'moving_{i}d_return_mean'] - (2 * indicators_df[f'moving_{i}d_return_mean'])

        # Simple Moving Average and Stationary Moving Average
        for i in [10, 20, 50, 100, 200]:
            indicators_df[f'sma_{i}d'] = indicators_df['close'].rolling(window=i).mean()
            indicators_df[f'sma_{i}d_shift'] = indicators_df[f'sma_{i}d'].shift(1)
            #indicators_df[f'sma_{i}d_coef'] = (-indicators_df[f'sma_{i}d'].shift(i) + indicators_df[f'sma_{i}d']) / 2
            #indicators_df[f'sma_{i}d_std'] = indicators_df['close'].rolling(window=i).std()
            #indicators_df[f'stationary_sma_{i}d'] = indicators_df['close'] - indicators_df[f'sma_{i}d']
            #indicators_df[f'stationary_sma_{i}d_zscore'] = indicators_df[f'stationary_sma_{i}d'] / indicators_df[f'sma_{i}d_std']
            #indicators_df[f'stationary_sma_{i}d_zscore_shift'] = indicators_df[f'stationary_sma_{i}d_zscore'].shift(5)
            #indicators_df[f'stationary_sma_{i}d_zscore_shift_2'] = indicators_df[f'stationary_sma_{i}d_zscore'].shift(10)
            #indicators_df[f'stationary_sma_{i}d_zscore_min'] = indicators_df[f'stationary_sma_{i}d_zscore'].rolling(window=i).min()
            #indicators_df[f'stationary_sma_{i}d_zscore_max'] = indicators_df[f'stationary_sma_{i}d_zscore'].rolling(window=i).max()

        #for i in [10, 20, 100, 200]:

        #    y = int(i/2)
        #    y2 = int(1.5 * i)

        #    indicators_df[f'stationary_sma_{i}d_zscore_min_shift_1'] = indicators_df[f'stationary_sma_{i}d_zscore_min'].shift(y)
        #    indicators_df[f'stationary_sma_{i}d_zscore_min_shift_2'] = indicators_df[f'stationary_sma_{i}d_zscore_min'].shift(i)
        #    indicators_df[f'stationary_sma_{i}d_zscore_min_shift_3'] = indicators_df[f'stationary_sma_{i}d_zscore_min'].shift(y2)
        #    indicators_df[f'stationary_sma_{i}d_zscore_min_all'] = indicators_df[[f'stationary_sma_{i}d_zscore_min_shift_1', f'stationary_sma_{i}d_zscore_min_shift_2', f'stationary_sma_{i}d_zscore_min_shift_3']].min(axis=1)
        #    indicators_df[f'stationary_sma_{i}d_zscore_min_all_shift'] = indicators_df[f'stationary_sma_{i}d_zscore_min_all'].shift(5)
        #    indicators_df[f'stationary_sma_{i}d_zscore_min_all_shift_2'] = indicators_df[f'stationary_sma_{i}d_zscore_min_all'].shift(10)

        for i in [10, 20, 50, 100, 200]:
            for y in [20, 50, 100, 200]:
                if i < y:
                    indicators_df[f'sma_{i}d_{y}d_ratio'] = indicators_df[f'sma_{i}d'] / indicators_df[f'sma_{y}d']
                    indicators_df[f'sma_{i}d_{y}d_ratio_shift'] = indicators_df[f'sma_{i}d_{y}d_ratio'].shift(1)
                    indicators_df[f'sma_{i}d_{y}d_ratio_shift_2'] = indicators_df[f'sma_{i}d_{y}d_ratio'].shift(2)
                    indicators_df[f'sma_{i}d_{y}d_ratio_shift_3'] = indicators_df[f'sma_{i}d_{y}d_ratio'].shift(3)
                    indicators_df[f'sma_{i}d_{y}d_ratio_shift_5'] = indicators_df[f'sma_{i}d_{y}d_ratio'].shift(5)
                    indicators_df[f'sma_{i}d_{y}d_ratio_shift_10'] = indicators_df[f'sma_{i}d_{y}d_ratio'].shift(10)
                    indicators_df[f'sma_{i}d_{y}d_ratio_coef_2d'] = (indicators_df[f'sma_{i}d_{y}d_ratio'] - indicators_df[f'sma_{i}d_{y}d_ratio_shift']) / 2
                    #indicators_df[f'sma_{i}d_{y}d_ratio_coef_3d'] = (indicators_df[f'sma_{i}d_{y}d_ratio'] - indicators_df[f'sma_{i}d_{y}d_ratio_shift_2']) / 2
                    #indicators_df[f'sma_{i}d_{y}d_ratio_coef_4d'] = (indicators_df[f'sma_{i}d_{y}d_ratio'] - indicators_df[f'sma_{i}d_{y}d_ratio_shift_3']) / 2
                    #indicators_df[f'sma_{i}d_{y}d_ratio_coef_5d'] = (indicators_df[f'sma_{i}d_{y}d_ratio'] - indicators_df[f'sma_{i}d_{y}d_ratio_shift_5']) / 2
                    #indicators_df[f'sma_{i}d_{y}d_ratio_coef_10d'] = (indicators_df[f'sma_{i}d_{y}d_ratio'] - indicators_df[f'sma_{i}d_{y}d_ratio_shift_10']) / 2
                    #indicators_df[f'sma_{i}d_{y}d_ratio_avg'] = indicators_df[f'sma_{i}d_{y}d_ratio'].rolling(window=200).mean()
                    #indicators_df[f'sma_{i}d_{y}d_ratio_std'] = indicators_df[f'sma_{i}d_{y}d_ratio'].rolling(window=200).std()
                    #for z, w in [(2,2), (1.5, 15), (1,1), (0.5, 'half')]:
                    #    indicators_df[f'sma_{i}d_{y}d_ratio_{w}std_up'] = indicators_df[f'sma_{i}d_{y}d_ratio_avg'] + (z * indicators_df[f'sma_{i}d_{y}d_ratio_std'])
                    #    indicators_df[f'sma_{i}d_{y}d_ratio_{w}std_down'] = indicators_df[f'sma_{i}d_{y}d_ratio_avg'] - (z * indicators_df[f'sma_{i}d_{y}d_ratio_std'])
                    #    indicators_df[f'sma_{i}d_{y}d_ratio_{w}std_up_diff'] = indicators_df[f'sma_{i}d_{y}d_ratio_{w}std_up'] - indicators_df[f'sma_{i}d_{y}d_ratio']
                    #    indicators_df[f'sma_{i}d_{y}d_ratio_{w}std_down_diff'] =  indicators_df[f'sma_{i}d_{y}d_ratio'] - indicators_df[f'sma_{i}d_{y}d_ratio_{w}std_down']

        # Get RSI
        indicators_df['momentum_rsi'] = ta.momentum.RSIIndicator(close=indicators_df['close'], window=14).rsi()
        #indicators_df['momentum_rsi_low'] = ta.momentum.RSIIndicator(close=indicators_df['low'], window=14).rsi()
        #indicators_df['momentum_rsi_high'] = ta.momentum.RSIIndicator(close=indicators_df['high'], window=14).rsi()

        for i in [''] : #, '_low', '_high']:
            # Create bins of rsi and label them
            #indicators_df[f'rsi_bins{i}'] = pd.cut(indicators_df[f'momentum_rsi{i}'], bins=bins, labels=labels)
            #indicators_df[f'rsi_bins{i}'] = pd.to_numeric(indicators_df[f'rsi_bins{i}'], errors='coerce')
            indicators_df[f'rsi_bins{i}'] = pd.to_numeric(indicators_df[f'momentum_rsi{i}'], errors='coerce')

        for y in [1, 2, 3]:
            # Create a shift of the bin, to compare current with the previous
            indicators_df[f'rsi_bins_shift_{y}d'] = indicators_df['rsi_bins'].shift(y).fillna(0)
            #indicators_df[f'rsi_bins_shift_{y}d_low'] = indicators_df['rsi_bins_low'].shift(y).fillna(0)
            #indicators_df[f'rsi_bins_shift_{y}d_high'] = indicators_df['rsi_bins_high'].shift(y).fillna(0)

        #for i in [35, 70, 105, 140, 175, 210]:
            # Get rsi std
        #    indicators_df[f'rsi_std_{i}'] = indicators_df['rsi_bins'].rolling(i).std()

            # Get min rsi
        #    indicators_df[f'rsi_{i}_min_bin'] = indicators_df['rsi_bins'].rolling(i).min()
        #    indicators_df[f'rsi_{i}_min_bin_low'] = indicators_df['rsi_bins_low'].rolling(i).min()
        #    indicators_df[f'rsi_{i}_min_bin_high'] = indicators_df['rsi_bins_high'].rolling(i).min()

        #for i in ['', '_low', '_high']:
            # Get Min rsi shift
        #    indicators_df[f'rsi_35_min_bin_shift_1d{i}'] = indicators_df[f'rsi_35_min_bin{i}'].shift(1)
        #    indicators_df[f'rsi_35_min_bin_shift_2d{i}'] = indicators_df[f'rsi_35_min_bin{i}'].shift(2)

        # Get shifted min rsi
        #for i in ['', '_low', '_high']:
        #    indicators_df[f'rsi_35_min_bin_shifted_1{i}'] = indicators_df[f'rsi_35_min_bin{i}'].shift(35)
        #    indicators_df[f'rsi_35_min_bin_shifted_2{i}'] = indicators_df[f'rsi_35_min_bin{i}'].shift(70)
        #    indicators_df[f'rsi_35_min_bin_shifted_3{i}'] = indicators_df[f'rsi_35_min_bin{i}'].shift(105)

        #indicators_df['rsi_70_min_bin_shifted_1'] = indicators_df['rsi_70_min_bin'].shift(70)
        #indicators_df['rsi_70_min_bin_shifted_2'] = indicators_df['rsi_70_min_bin'].shift(140)
        #indicators_df['rsi_70_min_bin_shifted_3'] = indicators_df['rsi_70_min_bin'].shift(210)

        # Moving Average RSI
        #for i in [3, 5, 10, 20, 30, 50, 100]:
        #    indicators_df[f'rsi_sma_{i}'] = indicators_df['rsi_bins'].rolling(i).mean()

        # Get max rsi
        for i in [35]: #, 70, 105, 140, 175, 210]:
            indicators_df[f'rsi_{i}_max_bin'] = indicators_df['rsi_bins'].rolling(i).max()
            #indicators_df[f'rsi_{i}_max_bin_high'] = indicators_df['rsi_bins_high'].rolling(i).max()

        # Median min rsi
        #indicators_df['rsi_median_min_2'] = (indicators_df['rsi_35_min_bin'] + indicators_df['rsi_70_min_bin']) / 2
        #indicators_df['rsi_median_min_4'] = (indicators_df['rsi_35_min_bin'] + indicators_df['rsi_70_min_bin'] +
        #                             indicators_df['rsi_105_min_bin'] + indicators_df['rsi_140_min_bin']) / 4
        #indicators_df['rsi_median_min_6'] = (indicators_df['rsi_35_min_bin'] + indicators_df['rsi_70_min_bin'] +
        #                             indicators_df['rsi_105_min_bin'] + indicators_df['rsi_140_min_bin'] +
        #                             indicators_df['rsi_175_min_bin'] + indicators_df['rsi_210_min_bin']) / 6

        # Difference Median RSI and RSI
        #for i in [2, 4, 6]:
        #    indicators_df[f'rsi_median_min_{i}_diff'] = indicators_df['rsi_bins'] - indicators_df[f'rsi_median_min_{i}']

        # Rsi Signals
        #for i in [1, 2, 3]:
        #    indicators_df[f'rsi_signal_start_35_{i}'] = np.where((indicators_df['rsi_bins'] == indicators_df[f'rsi_35_min_bin_shifted_{i}']), 1, 0)
        #    indicators_df[f'rsi_signal_start_70_{i}'] = np.where((indicators_df['rsi_bins'] == indicators_df[f'rsi_70_min_bin_shifted_{i}']), 1, 0)

        # Sum of signals
        #indicators_df['sum_rsi_signal_start'] = (indicators_df['rsi_signal_start_35_1'] + indicators_df['rsi_signal_start_35_2'] +
        #                                indicators_df['rsi_signal_start_35_3'] + indicators_df['rsi_signal_start_70_1'] +
        #                                indicators_df['rsi_signal_start_70_2'] + indicators_df['rsi_signal_start_70_3'] )

        # MACD
        macd = ta.trend.MACD(close=indicators_df['close'])
        indicators_df['macd_line'] = macd.macd()
        indicators_df['macd_hist'] = macd.macd_diff()
        indicators_df['macd_signal_line'] = macd.macd_signal()
        indicators_df['macd_hist_1d_shift'] = indicators_df['macd_hist'].shift(1)
        indicators_df['macd_hist_2d_shift'] = indicators_df['macd_hist'].shift(2)
        indicators_df['macd_hist_3d_shift'] = indicators_df['macd_hist'].shift(3)

        # Shift line
        indicators_df['macd_line_shift_1d'] = indicators_df['macd_line'].shift(1)
        indicators_df['macd_line_shift_2d'] = indicators_df['macd_line'].shift(2)
        indicators_df['macd_line_shift_3d'] = indicators_df['macd_line'].shift(3)
        indicators_df['macd_sig_line_shift_1d'] = indicators_df['macd_signal_line'].shift(1)
        indicators_df['macd_sig_line_shift_2d'] = indicators_df['macd_signal_line'].shift(2)
        indicators_df['macd_sig_line_shift_3d'] = indicators_df['macd_signal_line'].shift(3)

        # MACD - mins
        indicators_df['macd_hist_2d_min'] = indicators_df['macd_hist'].rolling(2).min()
        indicators_df['macd_hist_3d_min'] = indicators_df['macd_hist'].rolling(3).min()
        indicators_df['macd_hist_5d_min'] = indicators_df['macd_hist'].rolling(5).min()
        indicators_df['macd_hist_7d_min'] = indicators_df['macd_hist'].rolling(7).min()
        indicators_df['macd_hist_35d_min'] = indicators_df['macd_hist'].rolling(35).min()
        indicators_df['macd_hist_70d_min'] = indicators_df['macd_hist'].rolling(70).min()
        indicators_df['macd_hist_140d_min'] = indicators_df['macd_hist'].rolling(140).min()

        # MACD - min shifts
        indicators_df['macd_hist_70d_min_shift_1'] = indicators_df['macd_hist_70d_min'].shift(70)
        indicators_df['macd_hist_70d_min_shift_2'] = indicators_df['macd_hist_70d_min'].shift(140)

        # MACD - shift
        indicators_df['macd_hist_3d_min_shift'] = indicators_df['macd_hist_3d_min'].shift(1)
        indicators_df['macd_hist_5d_min_shift'] = indicators_df['macd_hist_5d_min'].shift(1)
        indicators_df['macd_hist_7d_min_shift'] = indicators_df['macd_hist_7d_min'].shift(1)

        # MACD - min - shift
        indicators_df['macd_hist_3d_min_diff'] = indicators_df['macd_hist_3d_min'] - indicators_df['macd_hist_3d_min_shift']
        indicators_df['macd_hist_5d_min_diff'] = indicators_df['macd_hist_5d_min'] - indicators_df['macd_hist_5d_min_shift']
        indicators_df['macd_hist_7d_min_diff'] = indicators_df['macd_hist_7d_min'] - indicators_df['macd_hist_7d_min_shift']

        # MACD - rolling std - avg
        indicators_df['macd_hist_std_35'] = indicators_df['macd_hist'].rolling(35).std()
        indicators_df['macd_hist_std_70'] = indicators_df['macd_hist'].rolling(70).std()
        indicators_df['macd_hist_std_140'] = indicators_df['macd_hist'].rolling(140).std()
        indicators_df['macd_hist_std_35_avg'] = indicators_df['macd_hist_std_35'].rolling(35).mean()
        indicators_df['macd_hist_std_70_avg'] = indicators_df['macd_hist_std_70'].rolling(70).mean()
        indicators_df['macd_hist_std_140_avg'] = indicators_df['macd_hist_std_140'].rolling(140).mean()
        indicators_df['macd_hist_avg_35'] = indicators_df['macd_hist'].rolling(35).mean()
        indicators_df['macd_hist_avg_35_min'] = indicators_df['macd_hist_avg_35'].rolling(35).min()
        indicators_df['macd_hist_avg_70'] = indicators_df['macd_hist'].rolling(70).mean()
        indicators_df['macd_hist_avg_70_min'] = indicators_df['macd_hist_avg_70'].rolling(70).min()
        indicators_df['macd_hist_avg_140'] = indicators_df['macd_hist'].rolling(140).mean()
        indicators_df['macd_hist_avg_140_min'] = indicators_df['macd_hist_avg_140'].rolling(140).min()

        # MACD - Growth
        indicators_df['macd_hist_3d_min_coef'] = (-indicators_df['macd_hist'].shift(3) + indicators_df['macd_hist'].shift(1)) / 2
        indicators_df['macd_hist_5d_min_coef'] = (-indicators_df['macd_hist'].shift(5) + indicators_df['macd_hist'].shift(1)) / 4
        indicators_df['macd_hist_7d_min_coef'] = (-indicators_df['macd_hist'].shift(7) + indicators_df['macd_hist'].shift(1)) / 6

        # MACD - Growth - Line
        indicators_df['macd_line_3d_min_coef'] = (-indicators_df['macd_line'].shift(3) + indicators_df['macd_line'].shift(1)) / 2
        indicators_df['macd_line_5d_min_coef'] = (-indicators_df['macd_line'].shift(5) + indicators_df['macd_line'].shift(1)) / 4
        indicators_df['macd_line_7d_min_coef'] = (-indicators_df['macd_line'].shift(7) + indicators_df['macd_line'].shift(1)) / 6

        # MACD - Growth - shift
        indicators_df['macd_hist_3d_min_coef_shift_1'] = indicators_df['macd_hist_3d_min_coef'].shift(1)
        indicators_df['macd_hist_3d_min_coef_shift_2'] = indicators_df['macd_hist_3d_min_coef'].shift(2)
        indicators_df['macd_hist_3d_min_coef_shift_3'] = indicators_df['macd_hist_3d_min_coef'].shift(3)

        # MACD - Growth - shift - Line
        indicators_df['macd_line_3d_min_coef_shift_1'] = indicators_df['macd_line_3d_min_coef'].shift(1)
        indicators_df['macd_line_3d_min_coef_shift_2'] = indicators_df['macd_line_3d_min_coef'].shift(2)
        indicators_df['macd_line_3d_min_coef_shift_3'] = indicators_df['macd_line_3d_min_coef'].shift(3)

        # Initialize Bollinger Bands Indicator
        indicator_bb = ta.volatility.BollingerBands(close=indicators_df["close"], window=14, window_dev=2)

        # Add Bollinger Bands features
        indicators_df['bb_bbm'] = indicator_bb.bollinger_mavg()
        indicators_df['bb_bbh'] = indicator_bb.bollinger_hband()
        indicators_df['bb_bbl'] = indicator_bb.bollinger_lband()
        indicators_df['bb_std'] = (indicators_df['bb_bbh'] - indicators_df['bb_bbm']) / 2
        #indicators_df['bb_std_avg_100'] = indicators_df['bb_std'].rolling(100).mean()
        #indicators_df['bb_bbl_diff_std'] = (indicators_df['close'] - indicators_df['bb_bbl']) / indicators_df['bb_std']
        #indicators_df['bb_bbh_diff_std'] = (indicators_df['close'] - indicators_df['bb_bbh']) / indicators_df['bb_std']

        # Add Bollinger Bands shift
        for i in [1, 2, 3]: #, 5, 10, 20, 30]:
            indicators_df[f'bb_bbm_{i}'] = indicators_df['bb_bbm'].shift(i)
            indicators_df[f'bb_bbh_{i}'] = indicators_df['bb_bbh'].shift(i)
            indicators_df[f'bb_bbl_{i}'] = indicators_df['bb_bbl'].shift(i)

        # SMA
        #sma = ta.momentum.AwesomeOscillatorIndicator(high=indicators_df['high'], low=indicators_df['low'])
        #indicators_df['sma_oscillator'] = sma.ao()

        # ADX
        #adx = ta.trend.ADXIndicator(high=indicators_df['high'], low=indicators_df['low'], close=indicators_df['close'])
        #indicators_df['adx'] = adx.adx()
        #indicators_df['adx_neg'] = adx.adx_neg()
        #indicators_df['adx_pos'] = adx.adx_pos()

        # Pivot Points
        #indicators_df['previous_week_high'] = indicators_df['high'].rolling(5).max()
        #indicators_df['previous_week_low'] = indicators_df['low'].rolling(5).min()
        #indicators_df['previous_week_close'] = indicators_df['close'].rolling(5).mean()
        #indicators_df['pivot_point'] = (indicators_df['previous_week_high'] + indicators_df['previous_week_low'] + indicators_df['previous_week_close']) / 3
        #indicators_df['support_one'] = (indicators_df['pivot_point'] * 2) - indicators_df['previous_week_high']
        #indicators_df['support_two'] = indicators_df['pivot_point'] - (indicators_df['previous_week_high'] - indicators_df['previous_week_low'])
        #indicators_df['resistance_one'] = (indicators_df['pivot_point'] * 2) - indicators_df['previous_week_low']
        #indicators_df['resistance_two'] = indicators_df['pivot_point'] + (indicators_df['previous_week_high'] - indicators_df['previous_week_low'])

        # Benchmarks
        #for benchmark in benchmark_columns:
        #    for i in [10, 20, 30, 60, 90, 180]:
        #        indicators_df[f'correlation_{benchmark}_{i}d'] = indicators_df['close'].rolling(i).corr(indicators_df[benchmark])

        # ROC
        #for i in [12, 24, 48, 90]:
        #    indicators_df[f'roc_{i}'] = ta.momentum.ROCIndicator(close=indicators_df['close'], window=i).roc()
        #    indicators_df[f'roc_{i}_shift'] = indicators_df[f'roc_{i}'].shift(1)
        #    indicators_df[f'roc_{i}_shift_2d'] = indicators_df[f'roc_{i}'].shift(2)
        #    indicators_df[f'roc_{i}_shift_3d'] = indicators_df[f'roc_{i}'].shift(3)
        #    indicators_df[f'roc_{i}_shift_5d'] = indicators_df[f'roc_{i}'].shift(5)

        # Remove first 14 days
        indicators_df = indicators_df.reset_index(drop=True)

        # Future Returns
        future = indicators_df[['date', 'close']]
        future['date'] = pd.to_datetime(future['date'])
        future.sort_values(by=['date'], ascending=True, inplace = True)

        for i in [1, 3, 5, 7, 14, 21, 30, 60, 90, 120]:
            future[f'next_{i}d_price'] = future['close'].shift(-i)
            future[f'next_{i}d_return'] = (future[f'next_{i}d_price'] / future['close'])

        # Drop close price
        future = future.drop('close', axis=1)

        # Merge output with future returns
        indicators_df['date'] = pd.to_datetime(indicators_df['date'])
        indicators_df = pd.merge(indicators_df, future, on='date', how='left')

        return indicators_df

def run_indicators_list(stocks_list, price_df, results_list):
    """
    Function to run indicators in parallel
    """
    # Loop through symbols and append indicators to list
    for symbol in stocks_list:
        print(symbol)
        symbol_indicators_df = run_indicators(price_df, symbol)
        if symbol_indicators_df != None:
            return results_list.append(symbol_indicators_df)

def indicators_parallel(stocks_list, price_df):
    """
    Main function to run indicators in parallel
    """
    # Create Pool with Processes and Manager
    pool = Pool(processes=cpu_count() - 1)

    # Create Chunks of Stocks
    no_of_elements = len(stocks_list) // 16
    stock_chunks = chunks(stocks_list, no_of_elements)

    # Create list to store results
    results_list = Manager().list()

    # Start running Pools
    [pool.apply_async(run_indicators_list, args=(chunk, price_df, results_list)) for chunk in stock_chunks]
    pool.close()
    pool.join()

    # Create dataframe with results
    indicators_df = pd.concat(results_list)

    return indicators_df

def main_indicators(prices_table, stocks_table, indicators_table):

    # Extract prices and symbols from tables
    price_df = prices_table.read_table_to_pandas(drop_upload_date=True)
    stocks_list = stocks_table.extract_column_to_list('symbol')[:32]

    # Calculate indicators
    indicators_df = indicators_parallel(stocks_list, price_df)

    # Append data to table
    indicators_table.load_data(data_to_load=indicators_df, id_columns=['symbol', 'date', 'upload_datetime'])

    # Remove duplicates
    indicators_table.remove_duplicates(id_column='id', partition_by_columns='symbol, date', order_by_column='upload_datetime')
