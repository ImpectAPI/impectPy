######
#
# This function returns an access token for the external API
#
######

# load packages
import requests
import time
import urllib
from impectPy.helpers import make_api_request


# define function
def getAccessToken(username: str, password: str) -> str:
    # create tokenURL
    token_url = "https://login.impect.com/auth/realms/release/protocol/openid-connect/token"

    # define request parameters
    login = 'client_id=api&grant_type=password&username=' + urllib.parse.quote(
        username) + '&password=' + urllib.parse.quote(password)

    # define request headers
    headers = {"body": login,
               "Content-Type": "application/x-www-form-urlencoded"}

    # request access token
    response = make_api_request(url=token_url, method="POST", headers=headers, data=login, json=None)

    # raise an HTTPError for a non-200 status code
    response.raise_for_status()

    # get access token from response
    token = response.json()["access_token"]
    return token