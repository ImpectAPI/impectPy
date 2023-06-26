######
#
# This function returns the response from an API call
#
######

# load packages
import requests
import time


def make_api_request(url: str, method: str, headers: dict = None, data: dict = None,
                     json: dict = None, session: requests.Session = None) -> requests.Response:

    # create session object
    with requests.Session() as session:

        # define number of retries
        max_retries = 3

        # define retry delay
        retry_delay = 1

        # try API call
        for i in range(max_retries):
            # execute GET method
            if method == 'GET':
                response = session.get(url, headers=headers)
            # execute POST method
            elif method == 'POST':
                response = session.post(url=url, headers=headers, data=data, json=json)
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