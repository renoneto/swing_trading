import pandas as pd
import requests
from datetime import datetime as dt

def all_orders(access_token,
                my_client):
    """
    Function to check for all orders
    """
    # define our headers
    header = {'Authorization':"Bearer {}".format(access_token),
              "Content-Type":"application/json"}

    # define the endpoint for Saved orders, including your account ID
    endpoint = "https://api.tdameritrade.com/v1/orders"

    # define the payload, in JSON format
    payload = {'accountId': my_client.account_number}

    # make a post, NOTE WE'VE CHANGED DATA TO JSON AND ARE USING POST
    content = requests.get(url = endpoint, json = payload, headers = header)

    # Convert response to JSON
    json_response = content.json()

    # Check if we have orders to look at
    if len(json_response) > 0:
        # Extract Values from json_response
        # First we need to define which keys to extract, because we don't need everything
        valid_columns = ['quantity', 'filledQuantity', 'orderId', 'cancelable', 'editable', 'status', 'enteredTime', 'orderLegCollection']

        # List of orders values
        all_orders = []

        # For each order, first create an empty list to store results
        for order in json_response:
            order_values = []
            # For each key in order
            for column in order.keys():
                # If it's a valid column
                if column in valid_columns:
                    # And it's not 'orderLegCollection'
                    if column != 'orderLegCollection':
                        # Store results
                        order_values.append(order[column])
                    else:
                        # If it's 'orderLegCollection' then we need to go one level below
                        for leg in order['orderLegCollection']:
                            # Go through underlying keys of orderLegCollection
                            for column_2 in leg.keys():
                                # If it's 'instrument' we need to go one level below again
                                if column_2 == 'instrument':
                                    for column_3 in leg[column_2].keys():
                                        order_values.append(leg[column_2][column_3])
                                # Otherwise, just store results
                                else:
                                    order_values.append(leg[column_2])
            # Append values of order into all_orders and go to the next order
            all_orders.append(order_values)

        # Now that we have the values, we need to extract the columns
        # Columns
        column_names = []

        # Go through keys of first order
        for column in json_response[0].keys():
            # If it's a valid column
            if column in valid_columns:
                # And it's not 'orderLegCollection'
                if column != 'orderLegCollection':
                    # Store column name
                    column_names.append(column)
                else:
                    # Otherwise, we need to go one level below
                    for leg in order['orderLegCollection']:
                        for column_2 in leg.keys():
                            # And if it's 'instrument', we need to go one level below again
                            if column_2 == 'instrument':
                                for column_3 in leg[column_2].keys():
                                    column_names.append(column_3)
                            else:
                                column_names.append(column_2)

        # Created DataFrame with orders
        orders_df = pd.DataFrame(all_orders, columns = column_names)
        orders_df['enteredTime'] = pd.to_datetime(orders_df['enteredTime'])

        return orders_df

    # If there aren't any orders, then return nothing
    else:
        print('No orders were found')
        return 0

def cancel_order(access_token,
                my_client,
                order_id):
    """
    Function to cancel order
    """

    # define our headers
    header = {'Authorization':"Bearer {}".format(access_token),
              "Content-Type":"application/json"}

    # define the endpoint for Saved orders, including your account ID
    endpoint = r"https://api.tdameritrade.com/v1/accounts/{}/orders/{}".format(my_client.account_number, order_id)

    # make a post, NOTE WE'VE CHANGED DATA TO JSON AND ARE USING POST
    content = requests.delete(url = endpoint, headers = header)

    # show the status code, we want 200
    return content.status_code


def buy(access_token,
        my_client,
        symbol,
        quantity):
    """
    Function to create buy order
    """

    # define our headers
    header = {'Authorization':"Bearer {}".format(access_token),
              "Content-Type":"application/json"}

    # define the endpoint for Saved orders, including your account ID
    endpoint = r"https://api.tdameritrade.com/v1/accounts/{}/orders".format(my_client.account_number)

    # define the payload, in JSON format
    payload = {'orderType':'MARKET',
               'session':'NORMAL',
               'duration':'DAY',
               'orderStrategyType':'SINGLE',
               'orderLegCollection':
               [
                   {'instruction':'Buy',
                    'quantity':quantity,
                    'instrument':
                        {'symbol':symbol,
                         'assetType':'EQUITY'}
                   }
               ]
              }


    # make a post, NOTE WE'VE CHANGED DATA TO JSON AND ARE USING POST
    content = requests.post(url = endpoint, json = payload, headers = header)

    # show the status code, we want 200
    return content.status_code

def sell_trailing_stop(access_token,
                       my_client,
                       symbol,
                       percentage_stop,
                       quantity):
    """
    Function to create sell trailing stop order
    """

    # define our headers
    header = {'Authorization':"Bearer {}".format(access_token),
              "Content-Type":"application/json"}

    # define the endpoint for Saved orders, including your account ID
    endpoint = r"https://api.tdameritrade.com/v1/accounts/{}/orders".format(my_client.account_number)

    # define the payload, in JSON format
    payload = {
                  "complexOrderStrategyType": "NONE",
                  "orderType": "TRAILING_STOP",
                  "session": "NORMAL",
                  "stopPriceLinkBasis": "AVERAGE",
                  "stopPriceLinkType": "PERCENT",
                  "stopPriceOffset": percentage_stop,
                  "duration": "DAY",
                  "orderStrategyType": "SINGLE",
                  "orderLegCollection": [
                    {
                      "instruction": "SELL",
                      "quantity": quantity,
                      "instrument": {
                        "symbol": symbol,
                        "assetType": "EQUITY"
                      }
                    }
                  ]
                }

    # make a post, NOTE WE'VE CHANGED DATA TO JSON AND ARE USING POST
    content = requests.post(url = endpoint, json = payload, headers = header)

    # show the status code, we want 200
    return content.status_code
