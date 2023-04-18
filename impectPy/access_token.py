######
#
# This function returns an access token for the external API
#
######

# load packages
import requests


# define function
def getAccessToken(username: str, password: str) -> str:
    # create tokenURL
    token_url = "https://login.impect.com/auth/realms/production/protocol/openid-connect/token"

    # define request headers
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # define request parameters as a dictionary
    data = {"client_id": "api",
            "grant_type": "password",
            "username": username,
            "password": password}

    # request access token
    response = requests.post(url=token_url, headers=headers, data=data, json=None)

    # raise an HTTPError for a non-200 status code
    response.raise_for_status()

    # get access token from response
    token = response.json()["access_token"]
    return token