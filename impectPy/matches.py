import pandas as pd
import re
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_dict, validate_response

######
#
# This function returns a dataframe with basic information
# for all matches for a given set of parameters
#
######


def getMatches(iteration: int, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getMatchesFromHost(iteration, connection, "https://api.impect.com")

# define function
def getMatchesFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # get match data
    matches = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/"
            f"{iteration}/matches",
        method="GET"
    )

    # get data from response
    matches = validate_response(response=matches, endpoint="Matches")

    # get squads data
    squads = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/"
            f"{iteration}/squads",
        method="GET"
    )

    # get data from response
    squads = validate_response(response=squads, endpoint="Squads")

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    )

    # get data from response
    countries = validate_response(response=countries, endpoint="Countries")

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
    matches = matches.rename(columns={"fifaName": "homeSquadCountryName"})

    matches = matches.merge(
        countries,
        left_on="awaySquadCountryId",
        right_on="id",
        suffixes=("", "_right")
    )
    matches = matches.rename(columns={"fifaName": "awaySquadCountryName"})

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
    data = unnest_mappings_dict(data)

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