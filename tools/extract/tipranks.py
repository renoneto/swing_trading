from numpy.core.numeric import nan
from requests import Session
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

from datetime import datetime

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '\
                         'AppleWebKit/537.36 (KHTML, like Gecko) '\
                         'Chrome/75.0.3770.80 Safari/537.36'}

def analyst_recommendation(symbol):
    """
    Search for symbol's link in Simply Wall Street
    """
    # Create Session
    s = Session()

    # Add headers
    s.headers.update(HEADERS)

    # JSON Key Field
    url = f'https://autocomplete.tipranks.com/api/Autocomplete/getAutoCompleteNoSecondary/?name={symbol}'

    # Request and transform response in json
    search = s.get(url)
    json = search.json()

    # If there's data
    if len(json) > 0:

        # Extract symbol and update url
        final_symbol = json[0]['value']
        url = f'https://www.tipranks.com/api/stocks/getData/?name={final_symbol}'

        # Extract data
        data = s.get(url)
        json = data.json()

        # Extract what we need
        consensus = json['portfolioHoldingData']['bestAnalystConsensus']['consensus']
        buys = json['portfolioHoldingData']['bestAnalystConsensus']['distribution']['buy']
        hold = json['portfolioHoldingData']['bestAnalystConsensus']['distribution']['hold']
        sells = json['portfolioHoldingData']['bestAnalystConsensus']['distribution']['sell']
        target = json['ptConsensus'][-1]['priceTarget']

        # Extract News Sentiment
        url = f'https://www.tipranks.com/api/stocks/getNewsSentiments/?ticker={final_symbol}'
        # Extract data
        data = s.get(url)
        json = data.json()

        # Extract sentiment
        try:
            sentiment = json['sentiment']['bullishPercent']
        except:
            sentiment = 'NAN'

        return [symbol, consensus, buys, hold, sells, target, sentiment]

    else:
        return [symbol, 0, 0, 0, 0, 0, sentiment]
