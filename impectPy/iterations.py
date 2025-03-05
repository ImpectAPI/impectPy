# load packages
import pandas as pd
import re
import requests
from typing import Optional
from impectPy.helpers import RateLimitedAPI, unnest_mappings_dict, validate_response

######
#
# This function returns a dataframe containing all competitionIterations available to the user
#
######
def getIterations(token: str, session: Optional[requests.Session] = None) -> pd.DataFrame:
    return getIterationsFromHost(token, session, "https://api.impect.com")

# define function
def getIterationsFromHost(token: str, session: Optional[requests.Session], host: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI(session)

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # request competition iteration information from API
    response = rate_limited_api.make_api_request_limited(
        f"{host}/v5/customerapi/iterations/",
        method="GET",
        headers=my_header
    )

    # get data from response
    data = validate_response(response, "Iterations")

    # unnest nested IdMapping column
    data = unnest_mappings_dict(data)

    # convert to pandas dataframe
    df = pd.json_normalize(data)

    # drop idMappings column
    df = df.drop("idMappings", axis = 1)

    # fix column names using regex
    df = df.rename(columns=lambda x: re.sub("[\._](.)", lambda y: y.group(1).upper(), x))

    # keep first entry for skillcorner, heimspiel and wyscout data
    df.skillCornerId = df.skillCornerId.apply(lambda x: x[0] if x else None)
    df.heimSpielId = df.heimSpielId.apply(lambda x: x[0] if x else None)
    df.wyscoutId = df.wyscoutId.apply(lambda x: x[0] if x else None)

    # sort iterations
    df = df.sort_values(by="id")

    # return dataframe
    return df