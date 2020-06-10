import requests
import pandas as pd

def get_account(my_client,
                access_token):
    """
    Get Account Information and Positions.
    """

    # Define Payload
    payload = {'apikey': my_client.key,
              'fields': 'positions,orders'}

    # endpoint
    endpoint = r'https://api.tdameritrade.com/v1/accounts/{}'.format(my_client.account_number)

    # headers
    headers = {'Authorization': "Bearer {}".format(access_token)}

     # get content
    content = requests.get(url=endpoint, params=payload, headers=headers)

    return content.json()

def current_position(my_client, access_token):
    """
    Extract Current Portfolio and Cash Available
    """
    # Get JSON Response
    json_response = get_account(my_client, access_token)

    # JSON Base
    json_base = json_response['securitiesAccount']

    # Extract All positions and create dataframe
    # Create empty list to store results
    all_positions = []

    # For each security in positions extract values and store in all_positions
    for security in json_base['positions']:
        position = []
        for column in security.keys():
            if column != 'instrument':
                position.append(security[column])
            else:
                for security_detail in security['instrument'].keys():
                    position.append(security['instrument'][security_detail])

        all_positions.append(position)

    # Also extract column names
    column_names = []

    # Select first position and extract column names
    first_position = json_base['positions'][0]
    for column in first_position.keys():
        if column != 'instrument':
            column_names.append(column)
        else:
            for security_detail in first_position[column].keys():
                column_names.append(security_detail)

    # Positions Dataframe
    position_df = pd.DataFrame(all_positions, columns=column_names)

    # Get Cash and Account Values
    inital_cash_available = json_base['initialBalances']['cashAvailableForTrading']
    inital_account_value = json_base['initialBalances']['accountValue']
    current_cash_available = json_base['currentBalances']['cashAvailableForTrading']
    current_account_value = current_cash_available + json_base['currentBalances']['longMarketValue']
    projected_cash_available = json_base['projectedBalances']['cashAvailableForTrading']

    # Dictionary with Portfolio Numbers
    cash_mv = {'inital_cash_available': inital_cash_available,
                'inital_account_value': inital_account_value,
                'current_cash_available': current_cash_available,
                'current_account_value': current_account_value,
                'projected_cash_available': projected_cash_available
                }

    return position_df, cash_mv
