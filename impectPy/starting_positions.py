# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, ImpectSession, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains the starting formations for a
# given match
#
######


def getStartingPositions(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getStartingPositionsFromHost(matches, connection, "https://api.impect.com")


# define function
def getStartingPositionsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # create list to store matches that are forbidden (HTTP 403)
    forbidden_matches = []

    # get match info
    def fetch_match_info(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Match Info")

    # create list to store dfs
    match_data_list = []
    for match in matches:
        match_data = safe_execute(
            fetch_match_info,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}",
            identifier=match,
            forbidden_list=forbidden_matches
        )
        match_data_list.append(match_data)
    match_data = pd.concat(match_data_list)

    # filter for matches that are unavailable
    unavailable_matches = match_data[match_data.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in unavailable_matches]

    # drop matches that are forbidden
    matches = [match for match in matches if match not in forbidden_matches]

    # configure warning format
    def no_line_formatter(message, category, filename, lineno, line):
        return f"Warning: {message}\n"
    warnings.formatwarning = no_line_formatter

    # raise exception if no matches remaining or report removed matches
    if len(matches) == 0:
        raise Exception("All supplied matches are unavailable or forbidden. Execution stopped.")
    if len(forbidden_matches) > 0:
        warnings.warn(f"The following matches are forbidden for the user: {forbidden_matches}")
    if len(unavailable_matches) > 0:
        warnings.warn(f"The following matches are not available yet and were ignored: {unavailable_matches}")

    # extract iterationIds
    iterations = list(match_data[match_data.lastCalculationDate.notnull()].iterationId.unique())

    # get players
    players_list = []
    for iteration in iterations:
        players = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/players",
            method="GET"
        ).process_response(
            endpoint="Players"
        )[["id", "commonname"]]
        players_list.append(players)
    players = pd.concat(players_list).drop_duplicates()
    player_map = players.set_index("id")["commonname"].to_dict()

    # get squads
    squads_list = []
    for iteration in iterations:
        squads = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        )[["id", "name"]]
        squads_list.append(squads)
    squads = pd.concat(squads_list).drop_duplicates()
    squad_map = squads.set_index("id")["name"].to_dict()

    # get matches
    matchplan_list = []
    for iteration in iterations:
        matchplan = getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        )
        matchplan_list.append(matchplan)
    matchplan = pd.concat(matchplan_list)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # extract shirt numbers
    shirt_numbers_home = match_data[["id", "squadHomeId", "squadHomePlayers"]].rename(
        columns={"squadHomePlayers": "players", "squadHomeId": "squadId"}
    )
    shirt_numbers_away = match_data[["id", "squadAwayId", "squadAwayPlayers"]].rename(
        columns={"squadAwayPlayers": "players", "squadAwayId": "squadId"}
    )

    # concat dfs
    shirt_numbers = pd.concat([shirt_numbers_home, shirt_numbers_away], axis=0).reset_index(drop=True)

    # unnest players column
    shirt_numbers = shirt_numbers.explode("players").reset_index(drop=True)

    # normalize the JSON structure into separate columns
    shirt_numbers = pd.concat(
        [
            shirt_numbers.drop(columns=["players"]),
            pd.json_normalize(shirt_numbers["players"]).rename(columns={"id": "playerId"})
        ],
        axis=1
    )

    # extract starting_positions
    starting_positions_home = match_data[["id", "squadHomeId", "squadHomeStartingPositions"]].rename(
        columns={"squadHomeStartingPositions": "squadStartingPositions", "squadHomeId": "squadId"}
    )
    starting_positions_away = match_data[["id", "squadAwayId", "squadAwayStartingPositions"]].rename(
        columns={"squadAwayStartingPositions": "squadStartingPositions", "squadAwayId": "squadId"}
    )

    # concat dfs
    starting_positions = pd.concat([starting_positions_home, starting_positions_away], axis=0).reset_index(drop=True)

    # unnest formations column
    starting_positions = starting_positions.explode("squadStartingPositions").reset_index(drop=True)

    # normalize the JSON structure into separate columns
    starting_positions = starting_positions.join(pd.json_normalize(starting_positions["squadStartingPositions"]))

    # drop the original column
    starting_positions.drop(columns=["squadStartingPositions"], inplace=True)

    # start merging dfs

    # merge substitutions with shirt numbers
    starting_positions = starting_positions.merge(
        shirt_numbers,
        left_on=["playerId", "squadId", "id"],
        right_on=["playerId", "squadId", "id"],
        how="left",
        suffixes=("", "_x")
    )

    # merge substitutions with squads
    starting_positions["squadName"] = starting_positions.squadId.map(squad_map)
    starting_positions["playerName"] = starting_positions.playerId.map(player_map)

    # merge with matches info
    starting_positions = starting_positions.merge(
        matchplan[[
            "id", "skillCornerId", "heimSpielId", "wyscoutId", "matchDayIndex",
            "matchDayName", "scheduledDate", "lastCalculationDate", "iterationId"
        ]],
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge with competition info
    starting_positions = starting_positions.merge(
        iterations[["id", "competitionName", "competitionId", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # rename some columns
    starting_positions = starting_positions.rename(columns={
        "id": "matchId",
        "scheduledDate": "dateTime",
    })

    # fix column types
    missing_shirt_numbers = starting_positions["shirtNumber"].isnull()
    if missing_shirt_numbers.any():
        print("Warning: The following players are missing a shirt number and will be set to None:")
        print(starting_positions[missing_shirt_numbers][["matchId", "squadName", "playerName"]].to_string(index=False))
    starting_positions["shirtNumber"] = starting_positions["shirtNumber"].astype("Int64")

    # define desired column order
    cols = [
        "matchId",
        "dateTime",
        "competitionId",
        "competitionName",
        "competitionType",
        "iterationId",
        "season",
        "matchDayIndex",
        "matchDayName",
        "squadId",
        "squadName",
        "playerId",
        "playerName",
        "shirtNumber",
        "position",
        "positionSide"
    ]

    # reorder data
    starting_positions = starting_positions[cols]

    # reorder rows
    starting_positions = starting_positions.sort_values(["matchId", "squadId", "playerId"])

    # return events
    return starting_positions