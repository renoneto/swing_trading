import requests
import pandas as pd
from datetime import datetime

import sys
# to import local fuctions
sys.path.insert(0, '../tools')

from keys.discord_keys import client_id
from extract.fundamentals import symbol_data, search_symbol
from extract.tipranks import analyst_recommendation

def send_message_to_discord(signal
                            , webhook=client_id.webhook):
    # Define path to file
    filepath = '/Users/renovieira/Desktop/swing_trading/output/trades.feather'

    # Read file
    trades = pd.read_feather(filepath)

    # Figure out today's date and keep only today's buys
    today = datetime.today().strftime("%Y-%m-%d")

    # Find today's buys
    buy_me = trades[(trades[signal] == True) & (trades['just_date'].astype(str) == today)]

    # Extract Price from Valuation
    results = []
    for symbol in buy_me['symbol']:

        recommendation = analyst_recommendation(symbol)
        results.append(recommendation)

    # Merge results
    analysts = pd.DataFrame(results, columns = ['symbol', 'consensus', 'buys', 'hold', 'sells', 'target', 'sentiment'])
    buy_me = pd.merge(buy_me, analysts, on =['symbol'], how='left')

    # Potential
    buy_me['potential'] = (((buy_me['target'] / buy_me['close_price_x']) - 1) * 100).round()

    # Create message
    buy_me = buy_me.fillna('nan')

    buy_me['string'] = (buy_me['symbol'].replace(' ', '') + ' @' + buy_me['close_price_x'].round(2).astype(str) +
                        ' // Consensus: ' + buy_me['consensus'].str.upper() + ' // Target: ' + buy_me['target'].astype(str) + ' (' +
                        buy_me['potential'].astype(str).str[:-2] + '%)' + ' // Bullish Sentiment: ' + buy_me['sentiment'].astype(str) +
                        ' // ' + buy_me['earnings_difference'].astype(int).astype(str) + ' days since earnings.')
    buy_me = buy_me.sort_values('potential', ascending=False)

    buy_me = buy_me['string'].to_list()
    buy_me_string = str(buy_me).strip('[]').replace("'", "").replace(', ', '\n')

    # Check if there's anything at all
    if len(buy_me_string) == 0:
        buy_me_string = 'No buys, my friend.'

    # Username
    username = 'Chris Trader - ' + signal

    # Send to discord channel
    header = {"Content-Type":"application/json"}
    payload = {'content': buy_me_string,
            'username': username,
            'avatar_url': 'https://i.kym-cdn.com/entries/icons/original/000/021/311/free.jpg'
                }

    content = requests.post(url = webhook, json = payload, headers = header)
