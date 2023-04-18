######
#
# This function returns a dataframe containing all competitionIterations available to the user
#
######

import requests
import pandas as pd
import re


# define function
def getCompetitions(token: str) -> pd.DataFrame:

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # request competition iteration information from API
    response = requests.get("https://api.impect.com/v4/customerapi/scouting/competitionIterations/", headers=my_header)

    # raise an HTTPError for a non-200 status code
    response.raise_for_status()

    # get data from response
    data = response.json()["data"]

    # convert to pandas dataframe
    df = pd.json_normalize(data)

    # fix column names using regex
    df = df.rename(columns=lambda x: re.sub("\.(.)", lambda y: y.group(1).upper(), x))

    # return dataframe
    return df