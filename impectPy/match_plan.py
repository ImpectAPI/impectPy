######
#
# This function returns a dataframe with basic information
# for all matches for a given set of parameters
#
######

import pandas as pd
import requests
import re
import time


# define function
def getMatchplan(competitionIterationId: int, token: str) -> pd.DataFrame:
    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # get competition iteration data
    response = make_api_request(
        url="https://api.impect.com/v4/customerapi/scouting/competitionIterations/"
            f"{competitionIterationId}",
        method="GET",
        headers=my_header)

    # raise an HTTPError for a non-200 status code
    response.raise_for_status()

    # get data from response
    data = response.json()["data"]

    # use list comprehension to extract matches from competition iteration steps
    matches = [match for step in data["competitionIterationSteps"] for match in step["matches"]]

    # convert to df
    matches = pd.json_normalize(matches)

    # fix column names using regex
    matches = matches.rename(columns=lambda x: re.sub("\.(.)", lambda y: y.group(1).upper(), x))
    matches = matches.rename(columns=lambda x: x.replace("Competition", ""))

    # return matches
    return matches


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
