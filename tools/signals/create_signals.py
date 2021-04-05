from datetime import datetime as dt, timedelta

import numpy as np
import pandas as pd

def benchmark_filter(indicators_df):
    """
    Function to create benchmark signals
    """

    # Create dataframe with symbol and industry
    benchmarks = indicators_df[(indicators_df['sector'] == 'Benchmark')]

    # Keep only necessary columns and merge back to indicators
    benchmarks = benchmarks[['just_date', 'industry', 'benchmark_check']]
    benchmarks.columns = ['just_date', 'industry', 'benchmark_indicator']
    indicators_df = pd.merge(indicators_df, benchmarks, how='left', on=['just_date', 'industry'])

    return indicators_df

def create_signals(indicators_df, number_of_signals):
    """
    Function to create Buy and Sell Signals
    """

    # Create empty columns
    for i in range(1, number_of_signals + 1):
        indicators_df[f'buy_signal?{i}'] = np.nan
        indicators_df[f'sell_signal?{i}'] = np.nan

    # EPS Validator
    indicators_df['eps_growing'] = np.nan
    indicators_df['eps_growing'].mask(
                                    (
                                        (indicators_df['eps_ttm_difference_0_60'] >= 0)
                                        & (indicators_df['eps_ttm_difference_60_120'] > 0)
                                        & (indicators_df['eps_ttm_difference_120_180'] > 0)
                                    )
                                    , True, inplace = True)

    indicators_df['eps_falling'] = np.nan
    indicators_df['eps_falling'].mask(
                                    (
                                        (indicators_df['eps_ttm_difference_0_60'] <= 0)
                                        & (indicators_df['eps_ttm_difference_60_120'] < 0)
                                        & (indicators_df['eps_ttm_difference_120_180'] < 0)
                                    )
                                    , True, inplace = True)

    # Down Market
    #indicators_df['avoid_down_market'] = np.nan
    #indicators_df['avoid_down_market'].mask(
    #                                (
    #                                    (indicators_df['sma_50_SPY_close_price'] < indicators_df['sma_20_SPY_close_price'])
    #                                )
    #                                , True, inplace =True)


    # Good Fundamentals from Experiments
    indicators_df['good_fundamentals'] = np.nan
    indicators_df['good_fundamentals'].mask(
                                    (
                                        (indicators_df['net_oper_cf_ttm'] > indicators_df['net_oper_cf_ttm_4Q']) &
                                        (indicators_df['total_revenue_ttm'] > indicators_df['total_revenue_ttm_4Q'])
                                    )
                                    , True, inplace=True)

    # Market Cap Filter
    indicators_df['market_cap_filter'] = np.nan
    indicators_df['market_cap_filter'].mask(indicators_df['market_cap_calc'] > 2000, True, inplace=True)

    # Volume Traded filter
    indicators_df['volume_traded_filter'] = np.nan
    indicators_df['volume_traded_filter'].mask(indicators_df['volume_traded'] > indicators_df['volume_traded_30_quantile'], True, inplace=True)

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
                                        & (indicators_df['eps_falling'] != True)
                                        #& (indicators_df['benchmark_indicator'] == True)
                                        & (indicators_df['good_fundamentals'] == True)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                        & (indicators_df['market_cap_filter'] == True)
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
                                        & (indicators_df['eps_falling'] != True)
                                        & (indicators_df['good_fundamentals'] == True)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                        & (indicators_df['market_cap_filter'] == True)
                                        #& (indicators_df['benchmark_indicator'] == True)
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
                                        & (indicators_df['eps_falling'] != True)
                                        & (indicators_df['good_fundamentals'] == True)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                        & (indicators_df['market_cap_filter'] == True)
                                        #& (indicators_df['benchmark_indicator'] == True)
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
                                        & (indicators_df['sma_10d_20d_ratio'] < 1.01)
                                        & (indicators_df['sma_20d_50d_ratio'] > indicators_df['sma_20d_50d_ratio_shift'])
                                        & (indicators_df['sma_10d_20d_ratio_shift'] > indicators_df['sma_10d_20d_ratio_shift_2'])
                                        & (indicators_df['sma_10d_20d_ratio'] > indicators_df['sma_10d_20d_ratio_shift_2'])
                                        & (indicators_df['sma_10d_20d_ratio'] > indicators_df['sma_20d_50d_ratio'])
                                        & (indicators_df['sma_20d_50d_ratio_coef_2d'] > 0.001)
                                        #& (indicators_df['sma_50d_100d_ratio'] > indicators_df['sma_50d_100d_ratio_shift_10'])
                                        & (indicators_df['eps_ttm_difference_0_60'] >= 0)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                        & (indicators_df['market_cap_filter'] == True)
                                        & (indicators_df['good_fundamentals'] == True)
                                        #& (indicators_df['close_price_x'] > indicators_df['sma_50d'])
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
                                        (indicators_df['sma_50d_200d_ratio'] > 1)
                                        & (indicators_df['sma_50d_200d_ratio_shift'] < 1)
                                        & (indicators_df['bb_bbh_diff_std'] < 0)
                                        & (indicators_df['rsi_bins'] < 60)
                                        #& (indicators_df['eps_falling'] != True)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                    )
                                    , True, inplace=True)


    '''
    Buy Signal 6:
    '''
    indicators_df['buy_signal?6'].mask(
                                    (
                                        #(indicators_df['sma_50d_200d_ratio'] < 1)
                                        #& (indicators_df['sma_50d_200d_ratio_shift'] > 1)
                                        #& (indicators_df['eps_falling'] != True)
                                        (indicators_df['rsi_bins_cross_down_35_avg'] == True)
                                        & (indicators_df['market_cap_filter'] == True)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                    )
                                    , True, inplace = True)

    '''
    Bug Signal 7:
    '''
    indicators_df['buy_signal?7'].mask(
                                    (
                                                    #(indicators_df['rsi_bins'] < indicators_df['rsi_35_max_bin'])
                                        (indicators_df['rsi_bins'] > indicators_df['rsi_bins_shift_3d'])
                                        & (indicators_df['macd_hist'] >= indicators_df['macd_hist_1d_shift'])
                                        & (indicators_df['macd_line_3d_min_coef'] > 0)
            #                            & (indicators_df['macd_line_3d_min_coef'] > indicators_df['macd_line_5d_min_coef'])
                                        & (indicators_df['macd_line'] > indicators_df['macd_signal_line'])
                                        & ((indicators_df['bb_bbm']) > indicators_df['close_price_x'])
                                        & (indicators_df['eps_falling'] != True)
                                        & (indicators_df['good_fundamentals'] == True)
                                        & (indicators_df['avg_volume_90d'] > 300000)
                                        & (indicators_df['market_cap_filter'] == True)
                                    )
                                    , True, inplace = True)

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
                                    |
                                        ((indicators_df['close_price_x'] < indicators_df['bb_bbm'])
                                        & (indicators_df['close_price_shift_1d'] > indicators_df['bb_bbm_1']))
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
                                       (indicators_df['sma_50d_200d_ratio'] < 1)
                                        & (indicators_df['sma_50d_200d_ratio_shift'] > 1)
                                    )
                                    , True, inplace = True)

    '''
    Sell Signal 6
    '''
    indicators_df['sell_signal?6'].mask(
                                    (
                                        #(indicators_df['sma_50d_200d_ratio'] > 1)
                                        #& (indicators_df['sma_50d_200d_ratio_shift'] < 1)
                                        #& (indicators_df['avg_volume_90d'] > 300000)
                                        indicators_df['rsi_bins_80_cross_up'] == True
                                    )
                                    , True, inplace=True)

    '''
    Sell Signal 7
    '''
    indicators_df['sell_signal?7'].mask(
                                    (
                                            (
                                                (indicators_df['macd_hist_1d_shift'] < ((2 * indicators_df['macd_hist_std_70']) + indicators_df['macd_hist_avg_70']))
                                            & (indicators_df['macd_hist'] > ((2 * indicators_df['macd_hist_std_70']) + indicators_df['macd_hist_avg_70']))
                                            )
                                        |
                                            (
                                                (indicators_df['macd_line'] > indicators_df['macd_signal_line'])
                                            & (indicators_df['macd_line_shift_1d'] < indicators_df['macd_sig_line_shift_1d'])
                                            & (indicators_df['macd_hist'] < indicators_df['macd_hist_1d_shift'])
                                            )
                                    )
                                    , True, inplace=True)
    return indicators_df

def add_earnings(trades):
    """
    Add earning dates to trades to avoid buying before earnings
    """
    # Convert data time
    trades['just_date'] = pd.to_datetime(trades['just_date'])

    # Update trades to merge with earnings
    # Create dataframe with all dates and symbols
    # Find list of unique symbols, max and min dates
    list_of_symbols = trades['symbol'].unique()
    max_date = trades['just_date'].max()
    min_date = trades['just_date'].min()

    # Create empty list to store results
    lst_of_dates = []

    # Moving date will be added to the list and initially is the min_date
    moving_date = min_date

    # While the statement is true, keep adding one day to moving date and add it to list
    while moving_date <= max_date:
        lst_of_dates.append(moving_date)
        moving_date = moving_date + timedelta(days=1)

    # Create a DataFrame from this and create a dataframe with all symbols
    all_dates_df = pd.DataFrame(lst_of_dates, columns=['just_date'])
    all_symbols = pd.DataFrame(list_of_symbols, columns=['symbol'])

    # Dummy columns to do a cross join
    all_dates_df['key'] = 0
    all_symbols['key'] = 0

    # Merge on dummy column
    all_dates_df = all_dates_df.merge(all_symbols, on='key', how='outer')
    all_dates_df = all_dates_df[['just_date', 'symbol']]

    # Update trades dataframe
    trades = pd.merge(all_dates_df, trades, on=['just_date', 'symbol'], how='left')

    # Read Earnings
    earnings = pd.read_csv('../docs/earnings.csv')
    earnings['earnings_date'] = pd.to_datetime(earnings['earnings_date']).dt.date
    earnings['earnings_date_60d'] = pd.to_datetime(earnings['earnings_date']) - timedelta(days=60)

    # Merge with trades
    trades = pd.merge(trades, earnings, how='left', left_on=['symbol', 'just_date'], right_on=['symbol', 'earnings_date_60d'])
    trades['earnings_date'] = trades.groupby('symbol').ffill()['earnings_date']
    trades['earnings_date'] = pd.to_datetime(trades['earnings_date'])
    trades['earnings_difference'] = trades['just_date'] - trades['earnings_date']
    trades['earnings_difference'] = trades['earnings_difference'].dt.days

    # Add Previous day price column for Trailing Loss calculation
    trades['previous_day_price'] = trades.groupby(['symbol']).shift(1)['close_price_x']

    # Drop Columns
    trades.drop(columns=['earnings_date_60d', 'earnings_date'], inplace=True)

    return trades


def main_signals(indicators_df, number_of_signals=7):

    print('Create Buy/Sell Signals')

    # Analyze benchmarks
    #indicators_df = benchmark_filter(indicators_df)

    # Create dataframe with signals
    trades = create_signals(indicators_df, number_of_signals=number_of_signals)

    # Keep only Columns that we need
    signals = []

    for column in trades.columns:
        if ('buy_signal' in column) | ('sell_signal' in column):
            signals.append(column)

    # Keep only a few columns on the trades file
    columns_to_keep = ['symbol', 'just_date', 'close_price_x']
    columns_to_keep += signals

    # Add earnings
    trades_output = add_earnings(trades[columns_to_keep])

    # Export
    trades_output.to_feather('../output/trades.feather')

    return trades
