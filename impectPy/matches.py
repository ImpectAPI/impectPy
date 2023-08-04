######
#
# This function returns a dataframe with basic information
# for all matches for a given set of parameters
#
######

import pandas as pd
import re
import requests
from typing import Optional
from impectPy.helpers import RateLimitedAPI, unnest_mappings


# define function
def getMatches(iteration: int, token: str, session: Optional[requests.Session] = None) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI(session)

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # get match data
    matches = rate_limited_api.make_api_request_limited(
        url="https://api.impect.com/v5/customerapi/iterations/"
            f"{iteration}/matches",
        method="GET",
        headers=my_header)

    # get data from response
    matches = matches.json()["data"]

    # get squads data
    squads = rate_limited_api.make_api_request_limited(
        url="https://api.impect.com/v5/customerapi/iterations/"
            f"{iteration}/squads",
        method="GET",
        headers=my_header)

    # get data from response
    squads = squads.json()["data"]

    # get country data
    countries = rate_limited_api.make_api_request_limited(
        url="https://api.impect.com/v5/customerapi/countries",
        method="GET",
        headers=my_header)

    # get data from response
    countries = countries.json()["data"]

    # convert to df and clean
    matches = clean_df(matches)
    squads = clean_df(squads)
    countries = pd.DataFrame(countries)

    # merge matches with squads
    matches = matches.merge(squads,
                            left_on="homeSquadId",
                            right_on="id",
                            suffixes=("", "_home"))
    matches = matches.rename(columns={
        "name": "homeSquadName",
        "type": "homeSquadType",
        "skillCornerId_home": "homeSquadSkillCornerId",
        "heimSpielId_home": "homeSquadHeimSpielId",
        "wyscoutId_home": "homeSquadWyscoutId",
        "countryId": "homeSquadCountryId"
    })
    matches = matches.merge(squads,
                            left_on="awaySquadId",
                            right_on="id",
                            suffixes=("", "_away"))
    matches = matches.rename(columns={
        "name": "awaySquadName",
        "type": "awaySquadType",
        "skillCornerId_away": "awaySquadSkillCornerId",
        "heimSpielId_away": "awaySquadHeimSpielId",
        "wyscoutId_away": "awaySquadWyscoutId",
        "countryId": "awaySquadCountryId"
    })

    # merge with countries
    matches = matches.merge(
        countries,
        left_on="homeSquadCountryId",
        right_on="id",
        suffixes=("", "_right")
    )
    matches = matches.rename(columns={"name": "homeSquadCountryName"})

    matches = matches.merge(
        countries,
        left_on="awaySquadCountryId",
        right_on="id",
        suffixes=("", "_right")
    )
    matches = matches.rename(columns={"name": "awaySquadCountryName"})

    # reorder columns
    matches = matches.loc[:, [
                                 'id',
                                 'skillCornerId',
                                 'heimSpielId',
                                 'wyscoutId',
                                 'iterationId',
                                 'matchDayIndex',
                                 'matchDayName',
                                 'homeSquadId',
                                 'homeSquadName',
                                 'homeSquadType',
                                 'homeSquadCountryId',
                                 'homeSquadCountryName',
                                 'homeSquadSkillCornerId',
                                 'homeSquadHeimSpielId',
                                 'homeSquadWyscoutId',
                                 'awaySquadId',
                                 'awaySquadName',
                                 'awaySquadType',
                                 'awaySquadCountryId',
                                 'awaySquadCountryName',
                                 'awaySquadSkillCornerId',
                                 'awaySquadHeimSpielId',
                                 'awaySquadWyscoutId',
                                 'scheduledDate',
                                 'lastCalculationDate',
                                 'available'
                             ]]

    # reorder matches
    matches = matches.sort_values(by=["matchDayIndex", "id"])

    # sort matches
    matches = matches.sort_values(by="id")

    # return matches
    return matches


# define function to clean df
def clean_df(data: dict) -> pd.DataFrame:
    # unnest nested idMapping key
    data = unnest_mappings(data)

    # convert to df
    df = pd.json_normalize(data)

    # fix column names using regex
    df = df.rename(columns=lambda x: re.sub("[\._](.)", lambda y: y.group(1).upper(), x))

    # drop idMappings column
    df = df.drop("idMappings", axis=1)

    # keep first entry for skillcorner and heimspiel data
    df.skillCornerId = df.skillCornerId.apply(lambda x: x[0] if x else None)
    df.heimSpielId = df.heimSpielId.apply(lambda x: x[0] if x else None)
    df.wyscoutId = df.wyscoutId.apply(lambda x: x[0] if x else None)

    return df
