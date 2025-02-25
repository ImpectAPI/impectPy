# load packages
import urllib
from impectPy.helpers import RateLimitedAPI

######
#
# This function returns an access token for the external API
#
######

@DeprecationWarning("This function is deprecated. Please use this method on the Impect instance.")
# define function
def getAccessToken(username: str, password: str) -> str:
    return getAccessTokenFromUrl(username, password, "https://login.impect.com/auth/realms/production/protocol/openid-connect/token")

def getAccessTokenFromUrl(username: str, password: str, token_url: str) -> str:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # define request parameters
    login = 'client_id=api&grant_type=password&username=' + urllib.parse.quote(
        username) + '&password=' + urllib.parse.quote(password)

    # define request headers
    headers = {"body": login,
               "Content-Type": "application/x-www-form-urlencoded"}

    # request access token
    response = rate_limited_api.make_api_request(url=token_url, method="POST", headers=headers, data=login, json=None)

    # get access token from response and return it
    token = response.json()["access_token"]
    return token