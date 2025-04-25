# load packages
import pandas as pd
import re
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_dict, validate_response

######
#
# This function returns a dataframe containing all competitionIterations available to the user
#
######


def getIterations(token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getIterationsFromHost(connection, "https://api.impect.com")

# define function
def getIterationsFromHost(connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # request competition iteration information from API
    response = connection.make_api_request_limited(
        f"{host}/v5/customerapi/iterations/",
        method="GET"
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

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )

    df = df.merge(
        countries[["id", "fifaName"]].rename(
            columns={"id": "competitionCountryId", "fifaName": "competitionCountryName"}
        ),
        how="left",
        on="competitionCountryId"
    )

    # sort iterations
    df = df.sort_values(by="id")

    # define column order
    order = [
        "id",
        "competitionId",
        "competitionName",
        "season",
        "competitionType",
        "competitionCountryId",
        "competitionCountryName",
        "competitionGender",
        "dataVersion",
        "lastChangeTimestamp",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
    ]

    # select columns
    df = df[order]

    # return dataframe
    return df