import pandas as pd
from impectPyRSCA.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_dict, validate_response

######
#
# This function returns a dataframe with basic information
# for all players for a given iteration
#
######


def getPlayers(iteration: int, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getPlayersFromHost(iteration, connection, "https://api.impect.com")

# define function
def getPlayersFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for iteration argument
    if not isinstance(iteration, int):
        raise Exception("Input for iteration argument must be an integer")

    # get player data
    players = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/"
            f"{iteration}/players",
        method="GET"
    )

    # get data from response
    players = validate_response(response=players, endpoint="Players")

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    )

    # get data from response
    countries = validate_response(response=countries, endpoint="Countries")

    # convert to df and clean
    players = clean_df(players)
    countries = pd.DataFrame(countries)

    # explode countryIds to get one row per country per player
    players_exploded = players.explode("countryIds").reset_index(drop=True)

    # merge with countries to get country names
    players_exploded = players_exploded.merge(
        countries,
        left_on="countryIds",
        right_on="id",
        suffixes=("", "_country")
    )

    # aggregate country info back to one row per player
    country_info = players_exploded.groupby("id").agg(
        countryNames=("fifaName", list)
    ).reset_index()

    # merge country names back to players
    players = players.merge(country_info, on="id", how="left")

    # reorder columns
    players = players[[
        "id",
        "commonname",
        "firstname",
        "lastname",
        "birthdate",
        "birthplace",
        "leg",
        "gender",
        "countryIds",
        "countryNames",
        "currentSquadId",
        "skillCornerId",
        "heimSpielId",
        "wyscoutId"
    ]]

    # sort by id
    players = players.sort_values(by="id").reset_index(drop=True)

    # return players
    return players


# define function to clean df
def clean_df(data: dict) -> pd.DataFrame:

    # unnest nested idMapping key
    data = unnest_mappings_dict(data)

    # convert to df
    df = pd.json_normalize(data)

    # fix column names using regex
    import re
    df = df.rename(columns=lambda x: re.sub("[\._](.)", lambda y: y.group(1).upper(), x))

    # drop idMappings column
    df = df.drop("idMappings", axis=1)

    # keep first entry for skillcorner and heimspiel data
    df.skillCornerId = df.skillCornerId.apply(lambda x: x[0] if x else None)
    df.heimSpielId = df.heimSpielId.apply(lambda x: x[0] if x else None)
    df.wyscoutId = df.wyscoutId.apply(lambda x: x[0] if x else None)

    return df
