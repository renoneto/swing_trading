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
    It does at the security-level, some of the metrics require this level of granularity
    """

    # Add ta features filling NaN values
    indicators_df = price_df[price_df['symbol'] == symbol]

    # Sector
    sector = df_2['sector'].unique()[0]

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
        for row in [0, 10, 20, 30, 40]:
            # Calculations
            df_2[f'low_min_d{row}'] = df_2['low_price'].shift(row).rolling(10).min()
            df_2[f'low_max_d{row}'] = df_2['low_price'].shift(row).rolling(10).max()
            df_2[f'high_min_d{row}'] = df_2['high_price'].shift(row).rolling(10).min()
            df_2[f'high_max_d{row}'] = df_2['high_price'].shift(row).rolling(10).max()
            df_2[f'close_min_d{row}'] = df_2['close_price'].shift(row).rolling(10).min()
            df_2[f'close_max_d{row}'] = df_2['close_price'].shift(row).rolling(10).max()
            df_2[f'open_min_d{row}'] = df_2['open_price'].shift(row).rolling(10).min()
            df_2[f'open_max_d{row}'] = df_2['open_price'].shift(row).rolling(10).max()

        # Average Volume and Volume Traded
        df_2['volume_traded'] = (df_2['close_price'] * df_2['volume']) / 1000000000

        for i in [30, 60, 90]:
            df_2[f'avg_volume_{i}d'] = df_2['volume'].rolling(i).mean()
            df_2[f'avg_volume_traded_{i}d'] = df_2['volume_traded'].rolling(i).mean()

        # Year and Week No from date
        #indicators_df['year'] = pd.DatetimeIndex(indicators_df['date']).year
        #indicators_df['week_no'] = pd.DatetimeIndex(indicators_df['date']).week
        #indicators_df['weekday_no'] = pd.DatetimeIndex(indicators_df['date']).weekday

        # Shift Price
        for i in [1, 2, 3, 4, 5, 10, 15, 20, 30]:
            df_2[f'close_price_shift_{i}d'] = df_2['close_price'].shift(i)
            df_2[f'high_price_shift_{i}d'] = df_2['high_price'].shift(i)
            df_2[f'low_price_shift_{i}d'] = df_2['low_price'].shift(i)
            df_2[f'open_price_shift_{i}d'] = df_2['open_price'].shift(i)

        # Daily return
        indicators_df['daily_return'] = (indicators_df['close'] / indicators_df['close_shift_1d'])

        # Calculate Past Returns
        for i in [1, 2, 3, 4, 5, 10, 20, 30, 60, 90, 120, 150, 180]:
            df_2[f'moving_{i}d_return'] = (df_2['daily_return']).rolling(window=i).apply(np.prod, raw=True)

        # Calculate min daily moves
        for i in [30, 60, 90]:
            df_2[f'moving_{i}d_return_mean'] = df_2['daily_return'].rolling(window=i).mean()
            df_2[f'moving_{i}d_return_std'] = df_2['daily_return'].rolling(window=i).std()
            df_2[f'moving_{i}d_min_return'] = df_2[f'moving_{i}d_return_mean'] - (2 * df_2[f'moving_{i}d_return_mean'])

        # Exponential Moving Average and Stationary Moving Average
        for i in [10, 20, 50, 100, 200]:

            # Define ema
            ema = ta.trend.EMAIndicator(close=df_2['close_price'], n=i)

            df_2[f'sma_{i}d'] = ema.ema_indicator()
            df_2[f'sma_{i}d_shift'] = df_2[f'sma_{i}d'].shift(1)
            df_2[f'sma_{i}d_shift_5d'] = df_2[f'sma_{i}d'].shift(5)
            df_2[f'sma_{i}d_coef'] = (-df_2[f'sma_{i}d'].shift(i) + df_2[f'sma_{i}d']) / 2
            df_2[f'sma_{i}d_coef_shift_3d'] = df_2[f'sma_{i}d_coef'].shift(3)
            df_2[f'sma_{i}d_coef_shift_5d'] = df_2[f'sma_{i}d_coef'].shift(5)
            df_2[f'sma_{i}d_coef_shift_10d'] = df_2[f'sma_{i}d_coef'].shift(10)
            df_2[f'sma_{i}d_std'] = df_2['close_price'].rolling(window=i).std()
            df_2[f'stationary_sma_{i}d'] = df_2['close_price'] - df_2[f'sma_{i}d']
            df_2[f'stationary_sma_{i}d_zscore'] = df_2[f'stationary_sma_{i}d'] / df_2[f'sma_{i}d_std']
            df_2[f'stationary_sma_{i}d_zscore_shift'] = df_2[f'stationary_sma_{i}d_zscore'].shift(5)
            df_2[f'stationary_sma_{i}d_zscore_shift_2'] = df_2[f'stationary_sma_{i}d_zscore'].shift(10)
            df_2[f'stationary_sma_{i}d_zscore_min'] = df_2[f'stationary_sma_{i}d_zscore'].rolling(window=i).min()
            df_2[f'stationary_sma_{i}d_zscore_max'] = df_2[f'stationary_sma_{i}d_zscore'].rolling(window=i).max()

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

        for y in [1, 2, 3, 5, 10, 15, 20, 30]:
            # Create a shift of the bin, to compare current with the previous
            indicators_df[f'rsi_bins_shift_{y}d'] = indicators_df['rsi_bins'].shift(y).fillna(0)
            #indicators_df[f'rsi_bins_shift_{y}d_low'] = indicators_df['rsi_bins_low'].shift(y).fillna(0)
            #indicators_df[f'rsi_bins_shift_{y}d_high'] = indicators_df['rsi_bins_high'].shift(y).fillna(0)

        for i in [35, 70, 105, 140, 175, 210]:
            # Get rsi std
            df_2[f'rsi_std_{i}'] = df_2['rsi_bins'].rolling(i).std()
            df_2[f'rsi_avg_{i}'] = df_2['rsi_bins'].rolling(i).mean()

            # Get min rsi
            df_2[f'rsi_{i}_min_bin'] = df_2['rsi_bins'].rolling(i).min()
        #    df_2[f'rsi_{i}_min_bin_low'] = df_2['rsi_bins_low'].rolling(i).min()
        #    df_2[f'rsi_{i}_min_bin_high'] = df_2['rsi_bins_high'].rolling(i).min()

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
            df_2[f'rsi_{i}_max_bin'] = df_2['rsi_bins'].rolling(i).max()
            df_2[f'rsi_{i}_std_bin'] = df_2['rsi_bins'].rolling(i).std()
            #df_2[f'rsi_{i}_max_bin_high'] = df_2['rsi_bins_high'].rolling(i).max()

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
        #for i in [1, 2, 3]:
        #    indicators_df[f'rsi_signal_start_35_{i}'] = np.where((indicators_df['rsi_bins'] == indicators_df[f'rsi_35_min_bin_shifted_{i}']), 1, 0)
        #    indicators_df[f'rsi_signal_start_70_{i}'] = np.where((indicators_df['rsi_bins'] == indicators_df[f'rsi_70_min_bin_shifted_{i}']), 1, 0)

        # Sum of signals
        #indicators_df['sum_rsi_signal_start'] = (indicators_df['rsi_signal_start_35_1'] + indicators_df['rsi_signal_start_35_2'] +
        #                                indicators_df['rsi_signal_start_35_3'] + indicators_df['rsi_signal_start_70_1'] +
        #                                indicators_df['rsi_signal_start_70_2'] + indicators_df['rsi_signal_start_70_3'] )

        # MACD
        macd = ta.trend.MACD(close=df_2['close_price'], n_slow=65, n_fast=30, n_sign=22)

        df_2['macd_line'] = macd.macd()
        df_2['macd_hist'] = macd.macd_diff()
        df_2['macd_signal_line'] = macd.macd_signal()

        for i in [1, 2, 3, 5, 10, 15, 20, 30]:

            # Shift Histogram, Line and Signal Line
            df_2[f'macd_hist_{i}d_shift'] = df_2['macd_hist'].shift(i)
            df_2[f'macd_line_shift_{i}d'] = df_2['macd_line'].shift(i)
            df_2[f'macd_sig_line_shift_{i}d'] = df_2['macd_signal_line'].shift(i)

        # MACD - mins
        #df_2['macd_hist_2d_min'] = df_2['macd_hist'].rolling(2).min()
        #df_2['macd_hist_3d_min'] = df_2['macd_hist'].rolling(3).min()
        #df_2['macd_hist_5d_min'] = df_2['macd_hist'].rolling(5).min()
        #df_2['macd_hist_7d_min'] = df_2['macd_hist'].rolling(7).min()
        #df_2['macd_hist_35d_min'] = df_2['macd_hist'].rolling(35).min()
        #df_2['macd_hist_70d_min'] = df_2['macd_hist'].rolling(70).min()
        #df_2['macd_hist_140d_min'] = df_2['macd_hist'].rolling(140).min()

        # MACD - min shifts
        #df_2['macd_hist_70d_min_shift_1'] = df_2['macd_hist_70d_min'].shift(70)
        #df_2['macd_hist_70d_min_shift_2'] = df_2['macd_hist_70d_min'].shift(140)

        # MACD - shift
        #df_2['macd_hist_3d_min_shift'] = df_2['macd_hist_3d_min'].shift(1)
        #df_2['macd_hist_5d_min_shift'] = df_2['macd_hist_5d_min'].shift(1)
        #df_2['macd_hist_7d_min_shift'] = df_2['macd_hist_7d_min'].shift(1)

        # MACD - min - shift
        #df_2['macd_hist_3d_min_diff'] = df_2['macd_hist_3d_min'] - df_2['macd_hist_3d_min_shift']
        #df_2['macd_hist_5d_min_diff'] = df_2['macd_hist_5d_min'] - df_2['macd_hist_5d_min_shift']
        #df_2['macd_hist_7d_min_diff'] = df_2['macd_hist_7d_min'] - df_2['macd_hist_7d_min_shift']

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
        indicator_bb = ta.volatility.BollingerBands(close=df_2["close_price"], n=70, ndev=2)

        # Add Bollinger Bands features
        df_2['bb_bbm'] = indicator_bb.bollinger_mavg()
        df_2['bb_bbh'] = indicator_bb.bollinger_hband()
        df_2['bb_bbl'] = indicator_bb.bollinger_lband()
        df_2['bb_std'] = (df_2['bb_bbh'] - df_2['bb_bbm']) / 2
        df_2['bb_std_avg_100'] = df_2['bb_std'].rolling(100).mean()
        df_2['bb_bbl_diff_std'] = (df_2['close_price'] - df_2['bb_bbl']) / df_2['bb_std']
        df_2['bb_bbh_diff_std'] = (df_2['close_price'] - df_2['bb_bbh']) / df_2['bb_std']

        # Add Bollinger Bands shift
        for i in [1, 2, 3, 5, 10]:
            df_2[f'bb_bbm_{i}'] = df_2['bb_bbm'].shift(i)
            df_2[f'bb_bbh_{i}'] = df_2['bb_bbh'].shift(i)
            df_2[f'bb_bbl_{i}'] = df_2['bb_bbl'].shift(i)
            df_2[f'bb_bbl_diff_std_{i}'] = df_2['bb_bbl_diff_std'].shift(i)
            df_2[f'bb_bbh_diff_std_{i}'] = df_2['bb_bbh_diff_std'].shift(i)

        for cross in [20, 30, 40, 50, 60, 70, 80, 90]:

            # Cross UP
            df_2[f'rsi_bins_{cross}_cross_up'] = False
            df_2[f'rsi_bins_{cross}_cross_up'].mask((df_2['rsi_bins'] > cross) & (df_2['rsi_bins_shift_2d'] < cross), True, inplace=True)

            # Cross DOWN
            df_2[f'rsi_bins_{cross}_cross_down'] = False
            df_2[f'rsi_bins_{cross}_cross_down'].mask((df_2['rsi_bins'] < cross) & (df_2['rsi_bins_shift_2d'] > cross), True, inplace=True)

        for mean in [35, 70, 105, 140, 175, 210]:

            # Cross UP mean
            df_2[f'rsi_bins_cross_up_{mean}_avg'] = False
            df_2[f'rsi_bins_cross_up_{mean}_avg'].mask((df_2['rsi_bins'] > df_2[f'rsi_avg_{mean}']) &
                                                        (df_2['rsi_bins_shift_3d'] < df_2[f'rsi_avg_{mean}']), True, inplace=True)

            # Cross DOWN mean
            df_2[f'rsi_bins_cross_down_{mean}_avg'] = False
            df_2[f'rsi_bins_cross_down_{mean}_avg'].mask((df_2['rsi_bins'] < df_2[f'rsi_avg_{mean}']) &
                                                        (df_2['rsi_bins_shift_3d'] > df_2[f'rsi_avg_{mean}']), True, inplace=True)

            for no_std in [1, 2]:

                # Cross UP mean + std
                df_2[f'rsi_bins_cross_up_{mean}_avg_plus_std{no_std}'] = False
                df_2[f'rsi_bins_cross_up_{mean}_avg_plus_std{no_std}'].mask((df_2['rsi_bins'] > (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))) &
                                                            (df_2['rsi_bins_shift_3d'] < (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

                # Cross DOWN mean + std
                df_2[f'rsi_bins_cross_down_{mean}_avg_plus_std{no_std}'] = False
                df_2[f'rsi_bins_cross_down_{mean}_avg_plus_std{no_std}'].mask((df_2['rsi_bins'] < (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))) &
                                                            (df_2['rsi_bins_shift_3d'] > (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

                # Cross UP mean + std
                df_2[f'rsi_bins_cross_up_{mean}_avg_minus_std{no_std}'] = False
                df_2[f'rsi_bins_cross_up_{mean}_avg_minus_std{no_std}'].mask((df_2['rsi_bins'] > (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))) &
                                                            (df_2['rsi_bins_shift_3d'] < (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

                # Cross DOWN mean + std
                df_2[f'rsi_bins_cross_down_{mean}_avg_minus_std{no_std}'] = False
                df_2[f'rsi_bins_cross_down_{mean}_avg_minus_std{no_std}'].mask((df_2['rsi_bins'] < (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))) &
                                                            (df_2['rsi_bins_shift_3d'] > (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

        for i in [10, 15, 20, 30]:
            # Bullish divergence
            df_2[f'rsi_bullish_divergence_{i}d'] = False
            df_2[f'rsi_bullish_divergence_{i}d'].mask(((df_2['close_price'] / df_2[f'close_price_shift_{i}d']) < 0.95) &
                                                    ((df_2['rsi_bins'] / df_2[f'rsi_bins_shift_{i}d']) > 1.05), True, inplace=True)

            # Bearish divergence
            df_2[f'rsi_bearish_divergence_{i}d'] = False
            df_2[f'rsi_bearish_divergence_{i}d'].mask(((df_2['close_price'] / df_2[f'close_price_shift_{i}d']) > 1.05) &
                                                    ((df_2['rsi_bins'] / df_2[f'rsi_bins_shift_{i}d']) < 0.95), True, inplace=True)



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
        df_2['timestamp'] = pd.to_datetime(df_2['timestamp'])
        df_2 = pd.merge(df_2, future, on='timestamp', how='left')

        # If Benchemark then do the following
        df_2['benchmark_check'] = np.nan

        if sector == 'Benchmark':
            df_2['benchmark_check'].mask(
                                    (df_2['sma_50d'] > df_2['sma_100d'])

                                    , True, inplace=True)

        return results_list.append(df_2)

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

def run_indicators_all(df_2):
    """
    Function to create metrics not at the security level.
    """

    # Volume Traded
    volume_traded = df_2[df_2['sector'] != 'Benchmark'].groupby('just_date')['volume_traded'].quantile(0.3).reset_index()
    volume_traded.columns = ['just_date', 'volume_traded_30_quantile']

    # Merge back
    df_2 = pd.merge(df_2, volume_traded, on='just_date', how='left')

    # RSI Metrics
    for cross in [20, 30, 40, 50, 60, 70, 80, 90]:

        # Cross UP
        df_2[f'rsi_bins_{cross}_cross_up'] = False
        df_2[f'rsi_bins_{cross}_cross_up'].mask((df_2['rsi_bins'] > cross) & (df_2['rsi_bins_shift_2d'] < cross), True, inplace=True)

        # Cross DOWN
        df_2[f'rsi_bins_{cross}_cross_down'] = False
        df_2[f'rsi_bins_{cross}_cross_down'].mask((df_2['rsi_bins'] < cross) & (df_2['rsi_bins_shift_2d'] > cross), True, inplace=True)

    for mean in [35, 70, 105, 140, 175, 210]:

        # Cross UP mean
        df_2[f'rsi_bins_cross_up_{mean}_avg'] = False
        df_2[f'rsi_bins_cross_up_{mean}_avg'].mask((df_2['rsi_bins'] > df_2[f'rsi_avg_{mean}']) &
                                                    (df_2['rsi_bins_shift_3d'] < df_2[f'rsi_avg_{mean}']), True, inplace=True)

        # Cross DOWN mean
        df_2[f'rsi_bins_cross_down_{mean}_avg'] = False
        df_2[f'rsi_bins_cross_down_{mean}_avg'].mask((df_2['rsi_bins'] < df_2[f'rsi_avg_{mean}']) &
                                                    (df_2['rsi_bins_shift_3d'] > df_2[f'rsi_avg_{mean}']), True, inplace=True)

        for no_std in [1, 2]:

            # Cross UP mean + std
            df_2[f'rsi_bins_cross_up_{mean}_avg_plus_std{no_std}'] = False
            df_2[f'rsi_bins_cross_up_{mean}_avg_plus_std{no_std}'].mask((df_2['rsi_bins'] > (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))) &
                                                        (df_2['rsi_bins_shift_3d'] < (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

            # Cross DOWN mean + std
            df_2[f'rsi_bins_cross_down_{mean}_avg_plus_std{no_std}'] = False
            df_2[f'rsi_bins_cross_down_{mean}_avg_plus_std{no_std}'].mask((df_2['rsi_bins'] < (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))) &
                                                        (df_2['rsi_bins_shift_3d'] > (df_2[f'rsi_avg_{mean}'] + (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

            # Cross UP mean + std
            df_2[f'rsi_bins_cross_up_{mean}_avg_minus_std{no_std}'] = False
            df_2[f'rsi_bins_cross_up_{mean}_avg_minus_std{no_std}'].mask((df_2['rsi_bins'] > (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))) &
                                                        (df_2['rsi_bins_shift_3d'] < (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

            # Cross DOWN mean + std
            df_2[f'rsi_bins_cross_down_{mean}_avg_minus_std{no_std}'] = False
            df_2[f'rsi_bins_cross_down_{mean}_avg_minus_std{no_std}'].mask((df_2['rsi_bins'] < (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))) &
                                                        (df_2['rsi_bins_shift_3d'] > (df_2[f'rsi_avg_{mean}'] - (no_std * df_2[f'rsi_std_{mean}']))), True, inplace=True)

    for i in [10, 15, 20, 30]:
        # Bullish divergence
        df_2[f'rsi_bullish_divergence_{i}d'] = False
        df_2[f'rsi_bullish_divergence_{i}d'].mask(((df_2['close_price_x'] / df_2[f'close_price_shift_{i}d']) < 0.95) &
                                                ((df_2['rsi_bins'] / df_2[f'rsi_bins_shift_{i}d']) > 1.05), True, inplace=True)

        # Bearish divergence
        df_2[f'rsi_bearish_divergence_{i}d'] = False
        df_2[f'rsi_bearish_divergence_{i}d'].mask(((df_2['close_price_x'] / df_2[f'close_price_shift_{i}d']) > 1.05) &
                                                ((df_2['rsi_bins'] / df_2[f'rsi_bins_shift_{i}d']) < 0.95), True, inplace=True)

    # Bollinger Bands
    for bb_type in ['bb_bbl', 'bb_bbm', 'bb_bbh']:

        # Cross UP
        df_2[f'{bb_type}_cross_up'] = False
        df_2[f'{bb_type}_cross_up'].mask((df_2['close_price_x'] > df_2[bb_type]) &
                                        (df_2['close_price_shift_3d'] < df_2[bb_type]), True, inplace=True)

        # Cross DOWN
        df_2[f'{bb_type}_cross_down'] = False
        df_2[f'{bb_type}_cross_down'].mask((df_2['close_price_x'] < df_2[bb_type]) &
                                        (df_2['close_price_shift_3d'] > df_2[bb_type]), True, inplace=True)

    # Buy Zone
    df_2['bb_buy_zone_4d'] = False
    df_2['bb_buy_zone_4d'].mask((df_2['bb_bbh_diff_std'] > -1) &
                            (df_2['bb_bbh_diff_std_1'] > -1) &
                            (df_2['bb_bbh_diff_std_2'] > -1) &
                            (df_2['bb_bbh_diff_std_3'] > -1), True, inplace=True)

    df_2['bb_buy_zone_3d'] = False
    df_2['bb_buy_zone_3d'].mask((df_2['bb_bbh_diff_std'] > -1) &
                            (df_2['bb_bbh_diff_std_1'] > -1) &
                            (df_2['bb_bbh_diff_std_2'] > -1), True, inplace=True)

    df_2['bb_buy_zone_6d'] = False
    df_2['bb_buy_zone_6d'].mask((df_2['bb_bbh_diff_std'] > -1) &
                            (df_2['bb_bbh_diff_std_1'] > -1) &
                            (df_2['bb_bbh_diff_std_2'] > -1) &
                            (df_2['bb_bbh_diff_std_3'] > -1) &
                            (df_2['bb_bbh_diff_std_5'] > -1), True, inplace=True)

    # End of Buy Zone
    df_2['bb_end_buy_zone'] = False
    df_2['bb_end_buy_zone'].mask((df_2['bb_bbh_diff_std'] < -1) &
                                (df_2['bb_bbh_diff_std_1'] > -1), True, inplace=True)

    # SMA Metrics
    for i in [10, 20, 50, 100, 200]:
        for y in [20, 50, 100, 200]:
            if i < y:

                # Crosses
                df_2[f'sma_{i}_{y}_cross'] = False
                df_2[f'sma_{i}_{y}_cross'].mask((df_2[f'sma_{i}d_{y}d_ratio'] > 1) &
                                                (df_2[f'sma_{i}d_{y}d_ratio_shift_3'] < 1), True, inplace=True)

    for i in [10, 20, 50, 100, 200]:
        # Crossovers
        df_2[f'sma_{i}_crossover'] = False
        df_2[f'sma_{i}_crossover'].mask((df_2[f'sma_{i}d'] > df_2['close_price_x']) &
                                        (df_2[f'sma_{i}d_shift'] < df_2['close_price_x']), True, inplace=True)

        # Price Above SMA
        df_2[f'price_above_sma_{i}'] = False
        df_2[f'price_above_sma_{i}'].mask(df_2['close_price_x'] > df_2[f'sma_{i}d'], True, inplace=True)

        # Stationary SMA Z-Score crosses
        df_2[f'sma_{i}_zscore_cross_2std'] = False
        df_2[f'sma_{i}_zscore_cross_2std'].mask((df_2[f'stationary_sma_{i}d_zscore'] > -2) &
                                            (df_2[f'stationary_sma_{i}d_zscore_shift'] < -2), True, inplace=True)
        df_2[f'sma_{i}_zscore_cross_3std'] = False
        df_2[f'sma_{i}_zscore_cross_3std'].mask((df_2[f'stationary_sma_{i}d_zscore'] > -3) &
                                            (df_2[f'stationary_sma_{i}d_zscore_shift'] < -3), True, inplace=True)

    # MACD Metrics
    for i in [10, 15, 20, 30]:
        # Bullish divergence
        df_2[f'macd_bullish_divergence_{i}d'] = False
        df_2[f'macd_bullish_divergence_{i}d'].mask(((df_2['close_price_x'] / df_2[f'close_price_shift_{i}d']) > 1.05) &
                                                ((df_2['macd_line'] / df_2[f'macd_line_shift_{i}d']) < 0.95), True, inplace=True)

        # Bearish divergence
        df_2[f'macd_bearish_divergence_{i}d'] = False
        df_2[f'macd_bearish_divergence_{i}d'].mask(((df_2['close_price_x'] / df_2[f'close_price_shift_{i}d']) < 0.95) &
                                                ((df_2['macd_line'] / df_2[f'macd_line_shift_{i}d']) > 1.05), True, inplace=True)

    # Price Movement
    df_2['higher_highs_3d'] = False
    df_2['higher_highs_3d'].mask((df_2['high_price'] > df_2['high_price_shift_1d']) &
                                (df_2['high_price_shift_1d'] > df_2['high_price_shift_2d']), True, inplace=True)

    df_2['higher_highs_3d_end'] = False
    df_2['higher_highs_3d_end'].mask((df_2['high_price'] < df_2['high_price_shift_1d']) &
                                (df_2['high_price_shift_1d'] > df_2['high_price_shift_2d']), True, inplace=True)

    df_2['higher_highs_4d'] = False
    df_2['higher_highs_4d'].mask((df_2['high_price'] > df_2['high_price_shift_1d']) &
                                (df_2['high_price_shift_1d'] > df_2['high_price_shift_2d']) &
                                (df_2['high_price_shift_2d'] > df_2['high_price_shift_3d']), True, inplace=True)

    df_2['higher_highs_4d_end'] = False
    df_2['higher_highs_4d_end'].mask((df_2['high_price'] < df_2['high_price_shift_1d']) &
                                (df_2['high_price_shift_1d'] > df_2['high_price_shift_2d']) &
                                (df_2['high_price_shift_2d'] > df_2['high_price_shift_3d']), True, inplace=True)

    df_2['lower_lows_3d'] = False
    df_2['lower_lows_3d'].mask((df_2['low_price'] < df_2['low_price_shift_1d']) &
                                (df_2['low_price_shift_1d'] < df_2['low_price_shift_2d']), True, inplace=True)

    df_2['lower_lows_3d_end'] = False
    df_2['lower_lows_3d_end'].mask((df_2['low_price'] > df_2['low_price_shift_1d']) &
                                (df_2['low_price_shift_1d'] < df_2['low_price_shift_2d']), True, inplace=True)

    df_2['lower_lows_4d'] = False
    df_2['lower_lows_4d'].mask((df_2['low_price'] < df_2['low_price_shift_1d']) &
                                (df_2['low_price_shift_1d'] < df_2['low_price_shift_2d']) &
                                (df_2['low_price_shift_2d'] < df_2['low_price_shift_3d']), True, inplace=True)

    df_2['lower_lows_4d_end'] = False
    df_2['lower_lows_4d_end'].mask((df_2['low_price'] > df_2['low_price_shift_1d']) &
                                (df_2['low_price_shift_1d'] < df_2['low_price_shift_2d']) &
                                (df_2['low_price_shift_2d'] < df_2['low_price_shift_3d']), True, inplace=True)

    return df_2

def main_indicators(all_prices, my_stocks_symbols, benchmark_columns, export=True):

    # Run full refresh of indicators at the security level
    indicators_df = indicators_parallel(all_prices, my_stocks_symbols, benchmark_columns)

    # Run indicators for all securities at the same time
    indicators_df = run_indicators_all(indicators_df)

    # Check if we want to export all indicators (this is a timely task)
    if export == True:
        print('Exporting Indicators')

        # Export indicators
        indicators_df['just_date'] = indicators_df['just_date'].astype(str)
        indicators_df.reset_index(drop=True).to_feather('../output/all_indicators.feather')

    return indicators_df
