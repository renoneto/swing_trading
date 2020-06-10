import requests
import pandas as pd

def get_transactions(access_token,
                     my_client):
    """
    Function to get historical transactions
    """

    # define our headers
    header = {'Authorization':"Bearer {}".format(access_token),
              "Content-Type":"application/json"}

    # define the endpoint for Saved orders, including your account ID
    endpoint = r'https://api.tdameritrade.com/v1/accounts/{}/transactions'.format(my_client.account_number)

    # make a post, NOTE WE'VE CHANGED DATA TO JSON AND ARE USING POST
    content = requests.get(url = endpoint, headers = header)

     # json response
    json_response = content.json()

    return json_response
