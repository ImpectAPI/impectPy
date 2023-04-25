######
#
# This function returns an access token for the external API
#
######

# load packages
import requests
import time


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
    response = make_api_request(url=token_url, method="POST", headers=headers, data=data, json=None)

    # raise an HTTPError for a non-200 status code
    response.raise_for_status()

    # get access token from response
    token = response.json()["access_token"]
    return token


def make_api_request(url: str, method: str, headers: dict = None, data: dict = None,
                     json: dict = None) -> requests.Response:
    # define number of retries
    max_retries = 3

    # define retry delay
    retry_delay = 1

    # try API call
    for i in range(max_retries):
        # execute GET method
        if method == 'GET':
            response = requests.get(url, headers=headers)
        # execute POST method
        elif method == 'POST':
            response = requests.post(url=url, headers=headers, data=data, json=json)
        # raise exception
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # check status code and return if 200
        if response.status_code == 200:
            return response
        # raise exception
        else:
            print(f"Received status code {response.status_code}, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    # return response
    return response
