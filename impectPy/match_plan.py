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
from .helpers import make_api_request


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
