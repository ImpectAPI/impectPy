# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df, ForbiddenError, safe_execute
from .matches import getMatchesFromHost
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
# This function returns a pandas dataframe that contains all scores for a
# given iteration and a given set of positions aggregated per player
#
######


def getPlayerIterationScores(
        iteration: int, token: str, positions: list = None, session: ImpectSession = ImpectSession()
) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getPlayerIterationScoresFromHost(iteration, connection, "https://api.impect.com", positions)

def getPlayerIterationScoresFromHost(
        iteration: int, connection: RateLimitedAPI, host: str, positions: list = None
) -> pd.DataFrame:

    # check input for iteration argument
    if not isinstance(iteration, int):
        raise Exception("Input for iteration argument must be an integer")

    # check input for positions argument
    if not isinstance(positions, list) and positions is not None:
        raise Exception("Input for positions argument must be a list")

    # create list to store matches that are forbidden (HTTP 403)
    forbidden_matches = []

    # check if the input positions are valid
    if positions is not None:
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
    squad_map = squads.set_index("id")["name"].to_dict()

    # get squadIds
    squad_ids = squads[squads.access].id.to_list()

    # get player match sums
    def fetch_player_iteration_scores(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Player Match Scores", raise_exception=False)

    # get player scores
    if positions is None:

        # create list to store dfs
        scores_list = []
        for squad_id in squad_ids:
            scores = safe_execute(
                fetch_player_iteration_scores,
                connection,
                url=f"{host}/v5/customerapi/iterations/{iteration}/squads/{squad_id}/player-scores",
                identifier=f"{squad_id}",
                forbidden_list=[]
            ).assign(
                iterationId=iteration,
                squadId=squad_id
            )

            # check if response is empty
            if len(scores) > 0:
                scores_list.append(scores)
        scores_raw = pd.concat(scores_list).reset_index(drop=True).reset_index(drop=True)

    else:

        # compile position string
        position_string = ",".join(positions)

        # create list to store dfs
        scores_list = []
        for squad_id in squad_ids:
            scores = safe_execute(
                fetch_player_iteration_scores,
                connection,
                url=f"{host}/v5/customerapi/iterations/{iteration}/"
                    f"squads/{squad_id}/positions/{position_string}/player-scores",
                identifier=f"{squad_id}",
                forbidden_list=[]
            ).assign(
                iterationId=iteration,
                squadId=squad_id,
                positions=position_string
            )

            # check if resonse is empty
            if len(scores) > 0:
                scores_list.append(scores)
        scores_raw = pd.concat(scores_list).reset_index(drop=True).reset_index(drop=True)

    # raise exception if no player played at given positions in entire iteration
    if len(scores_raw) == 0:
        raise Exception(f"No players played at given position in iteration {iteration}.")

    # print squads without players at given position
    if positions is not None:
        error_list = [str(squadId) for squadId in squad_ids if squadId not in scores_raw.squadId.to_list()]
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
        url=f"{host}/v5/customerapi/player-scores",
        method="GET"
    ).process_response(
        endpoint="playerScores"
    )[["id", "name"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    ).process_response(
        endpoint="Countries"
    )
    country_map = countries.set_index("id")["fifaName"].to_dict()

    # unnest scorings
    averages = scores_raw.explode("playerScores").reset_index(drop=True)

    # unnest dictionary in kpis column
    averages = pd.concat(
        [averages.drop(["playerScores"], axis=1), pd.json_normalize(averages["playerScores"])],
        axis=1
    )

    # merge with player scores to ensure all kpis are present
    averages = averages.merge(
        scores,
        left_on="playerScoreId",
        right_on="id",
        how="outer",
        suffixes=("", "_right")
    )

    # get matchShares
    if positions is None:
        match_shares = averages[
            ["iterationId", "squadId", "playerId", "position", "playDuration", "matchShare"]
        ].drop_duplicates()

        # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
        if len(averages["name"][averages["name"].isnull()]) > 0:
            averages["name"] = averages["name"].fillna("-1")

        # pivot kpi values
        averages = pd.pivot_table(
            averages,
            values="value",
            index=["iterationId", "squadId", "playerId", "position"],
            columns="name",
            aggfunc="sum",
            fill_value=0,
            dropna=False
        ).reset_index()

        # drop "-1" column
        if "-1" in averages.columns:
            averages.drop(["-1"], inplace=True, axis=1)

        # merge with playDuration and matchShare
        averages = averages.merge(
            match_shares,
            left_on=["iterationId", "squadId", "playerId", "position"],
            right_on=["iterationId", "squadId", "playerId", "position"],
            how="inner",
            suffixes=("", "_right")
        )
    else:
        match_shares = averages[
            ["iterationId", "squadId", "playerId", "positions", "playDuration", "matchShare"]
        ].drop_duplicates()

        # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
        if len(averages["name"][averages["name"].isnull()]) > 0:
            averages["name"] = averages["name"].fillna("-1")

        # pivot kpi values
        averages = pd.pivot_table(
            averages,
            values="value",
            index=["iterationId", "squadId", "playerId", "positions"],
            columns="name",
            aggfunc="sum",
            fill_value=0,
            dropna=False
        ).reset_index()

        # drop "-1" column
        if "-1" in averages.columns:
            averages.drop(["-1"], inplace=True, axis=1)

        # merge with playDuration and matchShare
        averages = averages.merge(
            match_shares,
            left_on=["iterationId", "squadId", "playerId", "positions"],
            right_on=["iterationId", "squadId", "playerId", "positions"],
            how="inner",
            suffixes=("", "_right")
        )

    # merge with other data
    averages["squadName"] = averages.squadId.map(squad_map)
    averages["playerCountry"] = averages.squadId.map(country_map)
    averages = averages.merge(
        iterations[["id", "competitionName", "season"]],
        left_on="iterationId",
        right_on="id",
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
    )

    # remove NA rows
    averages = averages[averages.iterationId.notnull()]

    # fix column types
    averages["squadId"] = averages["squadId"].astype(int)
    averages["playerId"] = averages["playerId"].astype(int)
    averages["iterationId"] = averages["iterationId"].astype(int)

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
        "positions" if positions is not None else "position",
        "matchShare",
        "playDuration"
    ]

    # add kpiNames to order
    order = order + scores.name.to_list()

    # select columns
    averages = averages[order]

    # fix some column types
    averages["squadId"] = averages["squadId"].astype("Int64")
    averages["playerId"] = averages["playerId"].astype("Int64")
    averages["wyscoutId"] = averages["wyscoutId"].astype("Int64")
    averages["heimSpielId"] = averages["heimSpielId"].astype("Int64")
    averages["skillCornerId"] = averages["skillCornerId"].astype("Int64")

    # return result
    return averages