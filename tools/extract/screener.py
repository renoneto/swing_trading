from bs4 import BeautifulSoup
from requests import Session
import pandas as pd

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '\
                         'AppleWebKit/537.36 (KHTML, like Gecko) '\
                         'Chrome/75.0.3770.80 Safari/537.36'}


def finviz_pull(stocks_table, url='https://finviz.com/screener.ashx?v=111&f=cap_midover&ft=4&o=volume'):
    """
    Function to scrape symbols out of Finviz's website based on the URL that was given.
    The output is a list of symbols with company name and industry.
    """
    # Create Session
    s = Session()

    # Add headers
    s.headers.update(HEADERS)

    # Extract data from Finviz - parse html
    screener = s.get(url)
    soup = BeautifulSoup(screener.text, 'html.parser')

    # Figure out number of stocks
    total_stocks_element = soup.find(class_ = 'count-text').text[7:]
    stop_position = total_stocks_element.find(' ')
    total_stocks = int(total_stocks_element[:stop_position])

    # Empty list to store stocks
    my_stocks = []

    # Pages and number of stocks
    page = 1
    stocks_imported = 0

    while stocks_imported < total_stocks:

        # Create new url
        new_url = url + '&r=' + str(page)

        # Pull data and parse html
        stock_data = s.get(new_url)
        soup = BeautifulSoup(stock_data.text, 'html.parser')

        # Table with stocks
        table_element_1 = soup.find_all(class_='table-dark-row-cp')
        table_element_2 = soup.find_all(class_='table-light-row-cp')
        table_element = table_element_1 + table_element_2

        # For each line extract the symbol, name and industry
        for idx, row in enumerate(table_element):

            # Creating table with all 'a' elements
            symbol_table = row.find_all('a')

            # Symbol
            symbol = symbol_table[1].text
            # Name
            symbol_name = symbol_table[2].text
            # Industry
            symbol_industry = symbol_table[3].text
            # Sector
            symbol_sector = symbol_table[4].text
            # Industry
            symbol_marketcap = symbol_table[6].text

            # Append all
            my_stocks.append([symbol, symbol_name, symbol_industry, symbol_sector, symbol_marketcap])

            stocks_imported += 1

        if stocks_imported == total_stocks:
            print(f"Total of {stocks_imported} stocks imported")
            print('Done loading')

        # Add 20 to page number, to go to next page. Each page contains 20 stocks.
        page += 20

    # Data Frame
    df = pd.DataFrame(my_stocks, columns = ['symbol', 'description', 'industry', 'sector', 'market_cap'])

    # Move it to the database
    stocks_table.load_data(df, id_columns=['symbol'], is_replace=True)
