import requests

def get_quotes(access_token,
                my_client,
                symbols):
    """
    Function to get quotes of a list of stocks
    """
    # Convert list to string
    str_symbols = ','.join(symbols)

    # define our headers
    header = {'Authorization':"Bearer {}".format(access_token),
              "Content-Type":"application/json"}

    # define the endpoint for Saved orders, including your account ID
    endpoint = 'https://api.tdameritrade.com/v1/marketdata/quotes'

    # payload with symbols
    payload = {'symbol': str_symbols,
               'apikey': my_client.key}

    # make a post, NOTE WE'VE CHANGED DATA TO JSON AND ARE USING POST
    content = requests.get(url = endpoint, headers = header, params=payload)

     # json response
    json_response = content.json()

    # quote symbols
    quote_symbols = list(json_response.keys())

    return json_response, quote_symbols
