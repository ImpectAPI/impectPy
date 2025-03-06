# load packages
import urllib
import requests
from impectPy.helpers import RateLimitedAPI

######
#
# This function returns an access token for the external API
#
######


# define function
def getAccessToken(username: str, password: str, session: requests.Session = requests.Session()) -> str:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    return getAccessTokenFromUrl(username, password, connection, "https://login.impect.com/auth/realms/production/protocol/openid-connect/token")

def getAccessTokenFromUrl(username: str, password: str, connection: RateLimitedAPI, token_url: str) -> str:

    # define request parameters
    login = 'client_id=api&grant_type=password&username=' + urllib.parse.quote(
        username) + '&password=' + urllib.parse.quote(password)

    # define request headers
    headers = {"body": login,
               "Content-Type": "application/x-www-form-urlencoded"}

    # request access token
    response = connection.make_api_request(url=token_url, method="POST", headers=headers, data=login, json=None)

    # get access token from response and return it
    token = response.json()["access_token"]
    return token