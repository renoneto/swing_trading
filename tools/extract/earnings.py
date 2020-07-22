from numpy.core.numeric import full
import pandas as pd
from yahoo_earnings_calendar import YahooEarningsCalendar

def extract_earnings(stocks_path='../docs/my_stocks.csv', full_refresh=True):
    """
    Function to extract earnings dates of stocks in csv file using Yahoo Finance
    """

    # Create instance of YahooFinance
    yec = YahooEarningsCalendar()

    # Read stocks and create list with them
    stocks = pd.read_csv(stocks_path)
    symbols = list(stocks['symbol'].unique())

    if full_refresh == False:
        # Read existing file with earnings data
        earnings_file = pd.read_csv('../docs/earnings.csv')

        # Create list with symbols
        existing_symbols = list(earnings_file['symbol'].unique())

        # If symbols exits, remove it from list
        for symbol in existing_symbols:
            if symbol in symbols:
                symbols.remove(symbol)

    # Calculate length
    length = len(symbols)

    # Create empty list to store results
    results = []

    # for each symbol in list
    for idx, symbol in enumerate(symbols):
        # Get json_response with dates
        try:
            json_response = yec.get_earnings_of(symbol)
            # for each element in json_response, extract the following
            for element in json_response:
                # Append results to list
                results.append([element['ticker'], element['startdatetime'], element['epsestimate'], element['epsactual'], element['epssurprisepct']])
            # Print completion
            print(str(round(((idx+1)/length), 2) * 100) + '%')
        except:
            continue

    if full_refresh == True:
        # Create Dataframe and export
        pd.DataFrame(results, columns = ['symbol', 'earnings_date', 'eps_estimate', 'eps_actual', 'eps_surprise']).to_csv('../docs/earnings.csv', index=0)

    else:
        # Concatenate with existing file
        new_symbols = pd.DataFrame(results, columns = ['symbol', 'earnings_date', 'eps_estimate', 'eps_actual', 'eps_surprise'])
        pd.concat([earnings_file, new_symbols]).to_csv('../docs/earnings.csv', index=0)

