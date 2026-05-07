import pandas as pd
import re
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_dict, validate_response

######
#
# This function returns a dataframe with basic information
# for all matches for a given set of parameters
#
######


def getMatches(iteration: int, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of all matches for the given iteration."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getMatchesFromHost(iteration, connection, "https://api.impect.com")

# define function
def getMatchesFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch all matches for the given iteration from the given host and return them as a DataFrame.

    Merges match records with squad and country data and sorts by match day and match ID.
    """
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
        "gender": "homeSquadGender",
        "skillCornerId_home": "homeSquadSkillCornerId",
        "heimSpielId_home": "homeSquadHeimSpielId",
        "wyscoutId_home": "homeSquadWyscoutId",
        "optaId_home": "homeSquadOptaId",
        "statsPerformId_home": "homeSquadStatsPerformId",
        "transfermarktId_home": "homeSquadTransfermarktId",
        "soccerdonnaId_home": "homeSquadSoccerdonnaId",
        "countryId": "homeSquadCountryId"
    })
    matches = matches.merge(squads,
                            left_on="awaySquadId",
                            right_on="id",
                            suffixes=("", "_away"))
    matches = matches.rename(columns={
        "name": "awaySquadName",
        "type": "awaySquadType",
        "gender": "awaySquadGender",
        "skillCornerId_away": "awaySquadSkillCornerId",
        "heimSpielId_away": "awaySquadHeimSpielId",
        "wyscoutId_away": "awaySquadWyscoutId",
        "optaId_away": "awaySquadOptaId",
        "statsPerformId_away": "awaySquadStatsPerformId",
        "transfermarktId_away": "awaySquadTransfermarktId",
        "soccerdonnaId_away": "awaySquadSoccerdonnaId",
        "countryId": "awaySquadCountryId"
    })

    # merge with countries
    matches = matches.merge(
        countries,
        left_on="homeSquadCountryId",
        right_on="id",
        suffixes=("", "_homeSquadCountry")
    )
    matches = matches.rename(columns={"fifaName": "homeSquadCountryName"})

    matches = matches.merge(
        countries,
        left_on="awaySquadCountryId",
        right_on="id",
        suffixes=("", "_awaySquadCountry")
    )
    matches = matches.rename(columns={"fifaName": "awaySquadCountryName"})

    # derive final goals per team based on resultType
    result_type_to_suffix = {
        "REGULAR": "FullTime",
        "EXTRA_TIME": "ExtraTime2",
        "PENALTIES": "Penalties",
    }
    for side, out_col in [("Home", "homeSquadGoals"), ("Away", "awaySquadGoals")]:
        matches[out_col] = pd.NA
        for result_type, suffix in result_type_to_suffix.items():
            src_col = f"goals{side}{suffix}"
            if src_col in matches.columns:
                mask = matches["resultType"] == result_type
                matches.loc[mask, out_col] = matches.loc[mask, src_col]
        matches[out_col] = matches[out_col].astype("Int64")

    # reorder columns
    matches = matches[[
        "id",
        "skillCornerId",
        "heimSpielId",
        "wyscoutId",
        "optaId",
        "statsPerformId",
        "transfermarktId",
        "soccerdonnaId",
        "iterationId",
        "matchDayIndex",
        "matchDayName",
        "stadiumId",
        "homeSquadId",
        "homeSquadName",
        "homeSquadType",
        "homeSquadGender",
        "homeSquadCountryId",
        "homeSquadCountryName",
        "homeSquadSkillCornerId",
        "homeSquadHeimSpielId",
        "homeSquadWyscoutId",
        "homeSquadOptaId",
        "homeSquadStatsPerformId",
        "homeSquadTransfermarktId",
        "homeSquadSoccerdonnaId",
        "awaySquadId",
        "awaySquadName",
        "awaySquadType",
        "awaySquadGender",
        "awaySquadCountryId",
        "awaySquadCountryName",
        "awaySquadSkillCornerId",
        "awaySquadHeimSpielId",
        "awaySquadWyscoutId",
        "awaySquadOptaId",
        "awaySquadStatsPerformId",
        "awaySquadTransfermarktId",
        "awaySquadSoccerdonnaId",
        "scheduledDate",
        "lastCalculationDate",
        "available",
        "homeSquadGoals",
        "awaySquadGoals",
        "result",
        "resultType"
    ]]

    # sort matches by match day, then by id within each match day
    matches = matches.sort_values(by=["matchDayIndex", "id"])

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

    # keep first entry for skillcorner, heimspiel, wyscout, opta, statsperform, transfermarkt and soccerdonna data
    df["skillCornerId"] = df["skillCornerId"].apply(lambda x: x[0] if x else None)
    df["heimSpielId"] = df["heimSpielId"].apply(lambda x: x[0] if x else None)
    df["wyscoutId"] = df["wyscoutId"].apply(lambda x: x[0] if x else None)
    df["optaId"] = df["optaId"].apply(lambda x: x[0] if x else None)
    df["statsPerformId"] = df["statsPerformId"].apply(lambda x: x[0] if x else None)
    df["transfermarktId"] = df["transfermarktId"].apply(lambda x: x[0] if x else None)
    df["soccerdonnaId"] = df["soccerdonnaId"].apply(lambda x: x[0] if x else None)

    return df