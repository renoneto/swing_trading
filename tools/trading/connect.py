import time
import urllib
import requests
from datetime import datetime as dt

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

#from credentials import client_id

def refresh_token(my_client):
    """
    Function to refresh token using my_client information
    """

    # define my browser and hide it
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-startup-window")
    browser = webdriver.Chrome(options=chrome_options)

    # define the components to build a URL
    method = 'GET'
    url = 'https://auth.tdameritrade.com/auth?'
    client_code = my_client.key + '@AMER.OAUTHAP'
    payload = {'response_type':'code',
               'redirect_uri':'http://localhost/test',
               'client_id': client_code }

    # build the URL and store it in a new variable
    p = requests.Request(method, url, params=payload).prepare()
    myurl = p.url

    # go to the URL
    browser.get(myurl)

    # define items to fillout form
    payload = {'username': my_client.username,
               'password': my_client.password}

    # Login using username and password
    user_email = browser.find_element_by_xpath('//*[@id="username"]')
    user_email.send_keys(payload['username'])
    user_password = browser.find_element_by_xpath('//*[@id="password"]')
    user_password.send_keys(payload['password'])
    user_password.send_keys(Keys.RETURN)
    time.sleep(1)

    # By pass dual authentication by extracting security question
    element = browser.find_element_by_xpath('//*[@id="authform"]/main/details')
    element.click()
    time.sleep(1)
    element = browser.find_element_by_xpath('//*[@id="stepup_0_secretquestion"]/div/input')
    element.click()
    time.sleep(1)
    question = browser.find_element_by_xpath('//*[@id="authform"]/main/div[2]/p[2]')
    question = question.text

    # figure out correct answer
    if my_client.question_1 in question:
        my_answer = my_client.answer_1
    elif my_client.question_2 in question:
        my_answer = my_client.answer_2
    elif my_client.question_3 in question:
        my_answer = my_client.answer_3
    else:
        my_answer = my_client.answer_4

    # insert answer
    enter_answer = browser.find_element_by_xpath('//*[@id="secretquestion"]')
    enter_answer.send_keys(my_answer)
    time.sleep(1)
    element = browser.find_element_by_xpath('//*[@id="accept"]')
    element.click()
    time.sleep(1)
    element = browser.find_element_by_xpath('//*[@id="accept"]')
    element.click()

    # get the url with the token
    new_url = browser.current_url

    # grab the part we need, and decode it
    code = urllib.parse.unquote(new_url.split('code=')[1])

    # close the browser
    browser.quit()

    # THE AUTHENTICATION ENDPOINT
    # define the endpoint
    url = r"https://api.tdameritrade.com/v1/oauth2/token"

    # define the headers
    headers = {"Content-Type":"application/x-www-form-urlencoded"}

    # define the payload
    payload = {'grant_type': 'authorization_code',
               'access_type': 'offline',
               'code': code,
               'client_id': client_code,
               'redirect_uri':'http://localhost/test'}

    # post the data to get the token
    authReply = requests.post(r'https://api.tdameritrade.com/v1/oauth2/token', headers = headers, data=payload)

    # convert it to a dictionary
    decoded_content = authReply.json()

    # grab the access_token
    access_token = decoded_content['access_token']

    return access_token

def connection_alive(my_client,
                     access_token):
    """
    Function to check whether we need to refresh token.
    """

    # Define Payload
    payload = {'apikey': my_client.key}

    # endpoint
    endpoint = 'https://api.tdameritrade.com/v1/userprincipals'

    # headers
    headers = {'Authorization': "Bearer {}".format(access_token)}

     # get content
    content = requests.get(url=endpoint, params=payload, headers=headers)

    # if status code is not 200 then we need to get a new token
    if content.status_code == 200:
        return access_token
    else:
        access_token = refresh_token(my_client)
        return access_token
