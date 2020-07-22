import urllib
import requests

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

def refresh_token(client_id):
    """
    Function to get token for stocktwits
    """

    # define the headers
    headers = {"Content-Type":"application/x-www-form-urlencoded"}

    # define the payload
    payload = {'client_id': client_id.client_id,
            'client_secret': client_id.client_secret,
            'response_type': 'token',
            'redirect_uri':'http://api.stocktwits.com',
            'prompt': 1,
            'scope': 'read'
            }

    # url page
    authorize_url = 'https://api.stocktwits.com/api/2/oauth/authorize'

    # post the data to get the token
    authReply = requests.post(authorize_url, headers = headers, data=payload)

    # get url
    login_url = authReply.url

    # use selenium to go login user
    # define my browser and hide it
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-startup-window")
    browser = webdriver.Chrome(options=chrome_options)

    # go to the URL
    browser.get(login_url)

    # fill out username and password fields
    user_name = browser.find_element_by_xpath('//*[@id="user_session_login"]')
    user_name.send_keys(client_id.user_name)
    user_password = browser.find_element_by_xpath('//*[@id="user_session_password"]')
    user_password.send_keys(client_id.password)
    user_password.send_keys(Keys.RETURN)

    # get the url with the token
    new_url = browser.current_url

    # grab the part we need, and decode it
    access_token = urllib.parse.unquote(new_url.split('access_token=')[1])

    # close the browser
    browser.quit()

    return access_token

def get_trending_stocks(client_id):
    """
    Function to get top 30 trending stocks from stocktwits
    """

    # get access token
    access_token = refresh_token(client_id)

    # define url
    trending_url = 'https://api.stocktwits.com/api/2/trending/symbols.json'

    # define the payload
    payload = {'access_token': access_token}

    # get trending stocks using token
    trending_stocks = requests.get(trending_url, params=payload)

    # convert response to json
    json_response = trending_stocks.json()

    return json_response
