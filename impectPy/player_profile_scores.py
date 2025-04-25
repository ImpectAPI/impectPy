# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df
from .iterations import getIterationsFromHost

# define the allowed positions
allowed_positions = [
  "GOALKEEPER",
  "LEFT_WINGBACK_DEFENDER",
  "RIGHT_WINGBACK_DEFENDER",
  "CENTRAL_DEFENDER",
  "DEFENSE_MIDFIELD",
  "CENTRAL_MIDFIELD",
  "ATTACKING_MIDFIELD",
  "LEFT_WINGER",
  "RIGHT_WINGER",
  "CENTER_FORWARD"
]

######
#
# This function returns a pandas dataframe that contains all profile scores
# for a given iteration and a given set of positions per player
#
######


def getPlayerProfileScores(
        iteration: int, positions: list, token: str, session: requests.Session = requests.Session()
) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getPlayerProfileScoresFromHost(iteration, positions, connection, "https://api.impect.com")

def getPlayerProfileScoresFromHost(
        iteration: int, positions: list, connection: RateLimitedAPI, host: str
) -> pd.DataFrame:

    # check input for iteration argument
    if not isinstance(iteration, int):
        raise Exception("Input for iteration argument must be an integer")

    # check input for positions argument
    if not isinstance(positions, list):
        raise Exception("Input for positions argument must be a list")

    # check if the input positions are valid
    invalid_positions = [position for position in positions if position not in allowed_positions]
    if len(invalid_positions) > 0:
        raise Exception(
            f"Invalid position(s): {', '.join(invalid_positions)}."
            f"\nChoose one or more of: {', '.join(allowed_positions)}"
        )

    # get squads
    squads = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
        method="GET"
    ).process_response(
        endpoint="Squads"
    )

    # get squadIds
    squad_ids = squads[squads.access].id.to_list()

    # compile position string
    position_string = ",".join(positions)

    # get player profile scores per squad
    profile_scores_raw = pd.concat(
        map(lambda squadId: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/"
                f"squads/{squadId}/positions/{position_string}/player-profile-scores",
            method="GET"
        ).process_response(
            endpoint="PlayerIterationScores",
            raise_exception=False
        ).assign(
            iterationId=iteration,
            squadId=squadId,
            positions=position_string
        ),
            squad_ids),
        ignore_index=True)

    # raise exception if no player played at given positions in entire iteration
    if len(profile_scores_raw) == 0:
        raise Exception(f"No players played at given position in iteration {iteration}.")

    # print squads without players at given position
    error_list = [str(squadId) for squadId in squad_ids if squadId not in profile_scores_raw.squadId.to_list()]
    if len(error_list) > 0:
        print(f"No players played at positions {positions} for iteration {iteration} for following squads:\n\t{', '.join(error_list)}")

    # get players
    players = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/players",
        method="GET"
    ).process_response(
        endpoint="Players"
    )[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg", "countryIds", "idMappings"]]

    # only keep first country id for each player
    country_series = players["countryIds"].explode().groupby(level=0).first()
    players["countryIds"] = players.index.to_series().map(country_series).astype("float").astype("Int64")
    players = players.rename(columns={"countryIds": "countryId"})

    # unnest mappings
    players = unnest_mappings_df(players, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get scores
    scores = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/player-profiles",
        method="GET"
    ).process_response(
        endpoint="playerProfiles"
    )[["name"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )

    # unnest scorings
    profile_scores = profile_scores_raw.explode("profileScores").reset_index(drop=True)

    # unnest dictionary in kpis column
    profile_scores = pd.concat(
        [profile_scores.drop(["profileScores"], axis=1), pd.json_normalize(profile_scores["profileScores"])],
        axis=1
    )

    # merge with player scores to ensure all kpis are present
    profile_scores = profile_scores.merge(
        scores,
        left_on="profileName",
        right_on="name",
        how="outer",
        suffixes=("", "_right")
    )

    # get matchShares
    match_shares = profile_scores[
        ["iterationId", "squadId", "playerId", "positions", "playDuration", "matchShare"]].drop_duplicates()

    # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
    if len(profile_scores["name"][profile_scores["name"].isnull()]) > 0:
        profile_scores["name"] = profile_scores["name"].fillna("-1")

    # pivot kpi values
    profile_scores = pd.pivot_table(
        profile_scores,
        values="value",
        index=["iterationId", "squadId", "playerId", "positions"],
        columns="name",
        aggfunc="sum",
        fill_value=0,
        dropna=False
    ).reset_index()

    # drop "-1" column
    if "-1" in profile_scores.columns:
        profile_scores.drop(["-1"], inplace=True, axis=1)

    # merge with playDuration and matchShare
    profile_scores = profile_scores.merge(
        match_shares,
        left_on=["iterationId", "squadId", "playerId", "positions"],
        right_on=["iterationId", "squadId", "playerId", "positions"],
        how="inner",
        suffixes=("", "_right")
    )
    # merge with other data
    profile_scores = profile_scores.merge(
        iterations[["id", "competitionName", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        squads[["id", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[[
            "id", "wyscoutId", "heimSpielId", "skillCornerId", "commonname",
            "firstname", "lastname", "birthdate", "birthplace", "countryId", "leg"
        ]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="playerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        countries.rename(columns={"fifaName": "playerCountry"}),
        left_on="countryId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # remove NA rows
    profile_scores = profile_scores[profile_scores.iterationId.notnull()]

    # fix column types
    profile_scores["squadId"] = profile_scores["squadId"].astype("Int64")
    profile_scores["playerId"] = profile_scores["playerId"].astype("Int64")
    profile_scores["iterationId"] = profile_scores["iterationId"].astype("Int64")
    profile_scores["wyscoutId"] = profile_scores["wyscoutId"].astype("Int64")
    profile_scores["heimSpielId"] = profile_scores["heimSpielId"].astype("Int64")
    profile_scores["skillCornerId"] = profile_scores["skillCornerId"].astype("Int64")

    # define column order
    order = [
        "iterationId",
        "competitionName",
        "season",
        "squadId",
        "squadName",
        "playerId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "playerName",
        "firstname",
        "lastname",
        "birthdate",
        "birthplace",
        "playerCountry",
        "leg",
        "positions",
        "matchShare",
        "playDuration"
    ]

    # add kpiNames to order
    order = order + scores.name.to_list()

    # select columns
    profile_scores = profile_scores[order]

    # return result
    return profile_scores