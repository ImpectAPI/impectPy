# load packages
import pandas as pd
import re
import requests
from typing import Optional
from impectPy.helpers import RateLimitedAPI

######
#
# This function returns a dataframe containing all competitionIterations available to the user
#
######


# define function
def getIterations(token: str, session: Optional[requests.Session] = None) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI(session)

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # request competition iteration information from API
    response = rate_limited_api.make_api_request_limited(
        "https://api.impect.com/v5/customerapi/iterations/",
        method="GET",
        headers=my_header
    )

    # get data from response
    data = response.json()["data"]

    # convert to pandas dataframe
    df = pd.json_normalize(data)

    # unnest nested IdMapping column
    df[["skillCornerId", "heimSpielId"]] = df["idMappings"].apply(pd.Series)

    # drop idMappings column
    df = df.drop("idMappings", axis = 1)

    # keep first entry for skillcorner and heimspiel data
    df.skillCornerId = df.skillCornerId.apply(lambda x: x["skill_corner"][0] if x["skill_corner"] else None)
    df.heimSpielId = df.heimSpielId.apply(lambda x: x["heim_spiel"][0] if x["heim_spiel"] else None)

    # fix column names using regex
    df = df.rename(columns=lambda x: re.sub("\.(.)", lambda y: y.group(1).upper(), x))

    # sort iterations
    df = df.sort_values(by="id")

    # return dataframe
    return df