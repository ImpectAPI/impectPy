# load packages
import urllib
from typing import Optional
from impectPy.helpers import RateLimitedAPI, ImpectSession

######
#
# This function returns an access token for the external API
#
######


# define function
def getAccessToken(username: str, password: str, session: Optional[ImpectSession] = None) -> str:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session if session is not None else ImpectSession())

    return getAccessTokenFromUrl(username, password, connection, "https://login.impect.com/auth/realms/production/protocol/openid-connect/token")

def getAccessTokenFromUrl(username: str, password: str, connection: RateLimitedAPI, token_url: str) -> str:

    # define request parameters
    login = 'client_id=api&grant_type=password&username=' + urllib.parse.quote(
        username) + '&password=' + urllib.parse.quote(password)

    # define request headers
    connection.session.headers.update({"body": login, "Content-Type": "application/x-www-form-urlencoded"})

    # request access token
    response = connection.make_api_request(url=token_url, method="POST", data=login)

    # remove headers again
    connection.session.headers.clear()

    # get access token from response and return it
    token = response.json()["access_token"]
    return token