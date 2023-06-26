######
#
# This function returns a dataframe containing all competitionIterations available to the user
#
######

import requests
import pandas as pd
import re
import time
from impectPy.helpers import make_api_request


# define function
def getIterations(token: str) -> pd.DataFrame:
    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # request competition iteration information from API
    response = make_api_request(
        "https://api.release.impect.com/v5/customerapi/iterations/",
        method="GET",
        headers=my_header
    )

    # raise an HTTPError for a non-200 status code
    response.raise_for_status()

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

    # return dataframe
    return df