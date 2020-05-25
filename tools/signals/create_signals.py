from datetime import datetime as dt, timedelta

import numpy as np
import pandas as pd

def create_signals(indicators_df, number_of_signals=5):
    """
    Function to create Buy and Sell Signals
    """

    # Create empty columns
    for i in range(1, number_of_signals + 1):
        indicators_df[f'buy_signal?{i}'] = np.nan
        indicators_df[f'sell_signal?{i}'] = np.nan

    # DEFINE BUY SIGNALS
    '''
    Buy Signal 1:
    - Not Buying when RSI is equal to rsi_35_max_bin (test RSI Levels is not a good idea)
    - Check if RSI is going up though, relative to two days before
    - Check if MACD Hist is going up by comparing it with previous day's
    - Check if the MACD Line coefficient is positive
    - Check if the MACD Line 3d coefficient is greater than 5d's
    - MACD line above MACD Signal Line is a bullish sign
    - Bollinger Bands: Average + 1std should be greater than current price, otherwise, we lost all rally
    '''
    indicators_df['buy_signal?1'].mask(
                                    (
                                        (indicators_df['rsi_bins'] < indicators_df['rsi_35_max_bin'])
                                        & (indicators_df['rsi_bins'] > indicators_df['rsi_bins_shift_2d'])
                                        & (indicators_df['macd_hist'] >= indicators_df['macd_hist_1d_shift'])
                                        & (indicators_df['macd_line_3d_min_coef'] > 0)
                                        & (indicators_df['macd_line_3d_min_coef'] > indicators_df['macd_line_5d_min_coef'])
                                        & (indicators_df['macd_line'] > indicators_df['macd_signal_line'])
                                        & ((indicators_df['bb_bbm'] + indicators_df['bb_std']) > indicators_df['close_price_x'])
                                    )
                                    , True, inplace = True)

    '''
    Buy Signal 2:
    - Bollinger Bands: Average + STD has to be greater than current price, otherwise, we may have lost all rally
    - Cross of MACD Line and MACD Signal Line: Bullish Signal when MACD Line crosses Signal
    '''
    indicators_df['buy_signal?2'].mask(
                                    (
                                        ((indicators_df['bb_bbm'] + indicators_df['bb_std']) > indicators_df['close_price_x'])
                                        & (indicators_df['macd_line'] > indicators_df['macd_signal_line'])
                                        & (indicators_df['macd_line'] < indicators_df['macd_sig_line_shift_3d'])
                                    )
                                    , True, inplace = True)

    '''
    Buy Signal 3:
    - RSI Bins going up is a bullish sign
    - Check if MACD Histogram is going up: Bullish Sign (previous day vs 3 days ago)
    - Check if MACD Histogram is going up: Bullish Sign (check today vs 3 days ago) - Consistency
    - Check if MACD Histogram is going up: Bullish Sign (check 2 days ago vs 3 days ago) - Consistency
    - MACD Hist between -3stds and -1.75stds
    - Find MACD Hist Recovering from bottom by looking at MACD Hist from three days ago which should be
      GREATER THAN 35d Avg minus 3 STDs
    - Find MACD Hist Recovering from bottom by looking at MACD Hist from three days ago which should be
      LESS THAN 35d Avg minus 1.75 STDs
    - Make sure that MACD Hist is less than zero: We're trying to find recovery from bottom
    '''
    indicators_df['buy_signal?3'].mask(
                                    (
                                        (indicators_df['rsi_bins'] > indicators_df['rsi_bins_shift_3d'])
                                        & (indicators_df['macd_hist_1d_shift'] > indicators_df['macd_hist_3d_shift'])
                                        & (indicators_df['macd_hist'] > indicators_df['macd_hist_3d_shift'])
                                        & (indicators_df['macd_hist_2d_shift'] > indicators_df['macd_hist_3d_shift'])
                                        & (indicators_df['macd_hist_3d_shift'] > (indicators_df['macd_hist_avg_35'] - (3 * indicators_df['macd_hist_std_35'])))
                                        & (indicators_df['macd_hist_3d_shift'] < (indicators_df['macd_hist_avg_35'] - (1.75 * indicators_df['macd_hist_std_35'])))
                                        & (indicators_df['macd_hist'] < 0)
                                    )
                                    , True, inplace = True)

    '''
    Buy Signal 4 - REVISIT - Not sure if it's a good idea:
    - Z-Score of the Stationary 10d SMA (Price - SMA) from 10 days ago should be less or equal
      to 80% of the Min among three different Z-scores (from 5, 10 or 15 days (if 10d)): Trying to find the
      bottom of the Z-Score.
    - Moving 7d return has to be positive
    - RSI Should be between min and max of 175 days > WTF
    - MACD Hist is going up: Bullish Sign
    '''
    indicators_df['buy_signal?4'].mask(
                                    (
                                        (indicators_df['sma_10d_20d_ratio'] > 0.99)
                                        &(indicators_df['sma_10d_20d_ratio'] < 1.01)
                                        &(indicators_df['sma_20d_50d_ratio'] > indicators_df['sma_20d_50d_ratio_shift'])
                                        &(indicators_df['sma_10d_20d_ratio_shift'] > indicators_df['sma_10d_20d_ratio_shift_2'])
                                        &(indicators_df['sma_10d_20d_ratio'] > indicators_df['sma_10d_20d_ratio_shift_2'])
                                        &(indicators_df['sma_10d_20d_ratio'] > indicators_df['sma_20d_50d_ratio'])
                                        &(indicators_df['sma_20d_50d_ratio_coef_2d'] > 0.001)
                                    )
                                    , True, inplace=True)

    '''
    Buy Signal 5:
    - Golden Cross Strategy (first and second rows): Find Points in which the 50d SMA crosses the 200d SMA
    - Close Price is above 200d SMA > Bullish Sign
    - MACD Hist is positive: Bullish Sign
    '''
    indicators_df['buy_signal?5'].mask(
                                    (
                                        (indicators_df['sma_200d'] < indicators_df['sma_50d'])
                                        &(indicators_df['sma_200d_shift'] > indicators_df['sma_50d_shift'])
                                        & (indicators_df['close_price_x'] > indicators_df['sma_200d'])
                                        & (indicators_df['macd_hist'] > 0)
                                    )
                                    , True, inplace=True)

    # DEFINE SELL SIGNALS

    '''
    Sell Signal 1:
    - Sell when RSI becomes rsi_35_max_bin
    - Check if RSI starts going down
    - Check if Histogram is going down (losing steam)
    - Sell if MACD Line Coefficient becomes negative
    - MACD line above MACD Signal Line is a bullish sign > Sell if MACD LINE is below Signal Line
    '''
    indicators_df['sell_signal?1'].mask(
                                    (
                                        ((indicators_df['rsi_bins'] == indicators_df['rsi_35_max_bin'])
                                        & (indicators_df['rsi_bins'] < indicators_df['rsi_bins_shift_2d']))
                                    |
                                        ((indicators_df['macd_hist'] < indicators_df['macd_hist_2d_shift'])
                                        & (indicators_df['macd_line_3d_min_coef'] < 0))
                                    |
                                        ((indicators_df['macd_line_shift_2d'] > indicators_df['macd_sig_line_shift_2d'])
                                        & (indicators_df['macd_line'] < indicators_df['macd_signal_line']))
                                    )
                                    , True, inplace=True)

    '''
    Sell Signal 2:
    - Bollinger Bands: Check if Bollinger Band is in overbought territory
    - Cross of MACD Line and MACD Signal Line: Bullish Signal when MACD Line crosses Signal
    '''
    indicators_df['sell_signal?2'].mask(
                                    (
                                        ((indicators_df['bb_bbm'] + indicators_df['bb_std']) < indicators_df['close_price_x'])
                                    |
                                        ((indicators_df['macd_line_shift_2d'] > indicators_df['macd_sig_line_shift_2d'])
                                        & (indicators_df['macd_line'] < indicators_df['macd_signal_line']))
                                    )
                                    , True, inplace=True)

    '''
    Sell Signal 3:
    - Check if Histogram is losing steam on recovering from bottom
    '''
    indicators_df['sell_signal?3'].mask(
                                    (
                                        (indicators_df['macd_hist'] < indicators_df['macd_hist_2d_shift'])
                                        & (indicators_df['macd_hist_1d_shift'] < indicators_df['macd_hist_2d_shift'])
                                    )
                                    , True, inplace = True)

    '''
    Sell Signal 4:
    - Check if the 10d Stationary SMA Z-Score is greater or equal to 80% of the max of the same metric
      over the last 50 days.
    - Check if MACD Histogram is going down
    '''
    indicators_df['sell_signal?4'].mask(
                                    (
                                        (indicators_df['sma_20d_50d_ratio'] < indicators_df['sma_20d_50d_ratio_shift'])
                                        &(indicators_df['sma_20d_50d_ratio_shift'] < indicators_df['sma_20d_50d_ratio_shift_2'])
                                    )
                                    , True, inplace = True)

    '''
    Sell Signal 5:
    - Sell When 50d SMA crosses the close price: End of bullish signal, it might still go up, but I rather stop here.
    '''
    indicators_df['sell_signal?5'].mask(
                                    (
                                       (indicators_df['close_price_shift'] > indicators_df['sma_50d_shift'])
                                        & (indicators_df['close_price_x'] < indicators_df['sma_50d'])
                                    )
                                    , True, inplace = True)

    return indicators_df

def main_signals(indicators_df, number_of_signals=5):

    print('Create Buy/Sell Signals')

    # Create dataframe with signals
    trades = create_signals(indicators_df, number_of_signals=number_of_signals)

    # Keep only Columns that we need
    signals = []

    for n in range(1,number_of_signals + 1):
        signals.append(f'buy_signal?{n}')
        signals.append(f'sell_signal?{n}')

    # Keep only a few columns on the trades file
    columns_to_keep = ['symbol', 'just_date', 'close_price_x']
    columns_to_keep += signals

    # Export trades
    trades[columns_to_keep].to_csv('../output/trades.csv', index=0)

    return trades
