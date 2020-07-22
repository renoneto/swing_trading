import pandas as pd

def signal_track_record(trades, num_of_signals=5):
    """
    Track performance of signal in the past for each security
    """

    # List to store results
    results_list = []

    # Create List of signals
    signals_list = []
    [signals_list.append(f'buy_signal?{i}') for i in range(1,num_of_signals+1)]

    # For each signal
    for signal in signals_list:

        # All Buys so far
        allbuys = trades[(trades[signal] == True)][['just_date', 'symbol', 'next_21d_return']]

        # Successful Buys
        sucess = trades[(trades[signal] == True) & (trades['next_21d_return'] > 1.10)][['just_date', 'symbol']]
        sucess['success'] = 1

        # Fails
        fail = trades[(trades[signal] == True) & (trades['next_21d_return'] < 0.9)][['just_date', 'symbol']]
        fail['fail'] = 1

        # Results
        results = pd.merge(allbuys, sucess, on = ['just_date', 'symbol'], how='left')
        results = pd.merge(results, fail, on = ['just_date', 'symbol'], how='left')

        # Cumulative Sum of Successful and Fails
        results['cumsum_success'] = results.groupby(['symbol']).cumsum()['success'].ffill()
        results['cumsum_fail'] = results.groupby(['symbol']).cumsum()['fail'].ffill()
        results = results.fillna(0)

        # Total number of buys and win rate
        results['total_buy_running'] = results['cumsum_success'] + results['cumsum_fail']
        results['win_rate'] = results['cumsum_success'] / results['total_buy_running']

        # Append results to list
        results_list.append(results)

    # Create Dataframe
    df = pd.concat(results_list)

    return df
