# load packages
import numpy as np
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost
import re


######
#
# This function returns a pandas dataframe that contains all events for a
# given match
#
######


def getFormations(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getFormationsFromHost(matches, connection, "https://api.impect.com")


# define function
def getFormationsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # get match info
    matches = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Iterations"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = matches[matches.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = matches[~matches.id.isin(fail_matches)]

    # raise exception if no matches remaining or report removed matches
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(matches[matches.lastCalculationDate.notnull()].iterationId.unique())

    # get squads
    squads = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # extract formations
    formations_home = matches[["id", "squadHomeId", "squadHomeFormations"]].rename(
        columns={"squadHomeFormations": "squadFormations", "squadHomeId": "squadId"}
    )
    formations_away = matches[["id", "squadAwayId", "squadAwayFormations"]].rename(
        columns={"squadAwayFormations": "squadFormations", "squadAwayId": "squadId"}
    )

    # concat dfs
    formations = pd.concat([formations_home, formations_away], axis=0).reset_index(drop=True)

    # unnest formations column
    formations = formations.explode("squadFormations").reset_index(drop=True)

    # normalize the JSON structure into separate columns
    formations = formations.join(pd.json_normalize(formations["squadFormations"]))

    # drop the original column
    formations.drop(columns=["squadFormations"], inplace=True)

    # start merging dfs

    # merge formations with squads
    formations = formations.merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "squadName"}),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    )

    # merge with matches info
    formations = formations.merge(
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
    formations = formations.merge(
        iterations[["id", "competitionName", "competitionId", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # rename some columns
    formations = formations.rename(columns={
        "id": "matchId",
        "scheduledDate": "dateTime"
    })

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
        "gameTime",
        "gameTimeInSec",
        "formation"
    ]

    # reorder data
    formations = formations[cols]

    # reorder rows
    formations = formations.sort_values(["matchId", "squadId", "gameTimeInSec"])

    # return events
    return formations


######
#
# This function returns a pandas dataframe that contains all substitutions for a
# given match
#
######


def getSubstitutions(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSubstitutionsFromHost(matches, connection, "https://api.impect.com")


# define function
def getSubstitutionsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # get match info
    matches = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Iterations"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = matches[matches.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = matches[~matches.id.isin(fail_matches)]

    # raise exception if no matches remaining or report removed matches
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(matches[matches.lastCalculationDate.notnull()].iterationId.unique())

    # get squads
    squads = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get players
    players = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/players",
            method="GET"
        ).process_response(
            endpoint="Players"
        ),
            iterations),
        ignore_index=True)[["id", "commonname"]].drop_duplicates()

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # extract shirt numbers
    shirt_numbers_home = matches[["id", "squadHomeId", "squadHomePlayers"]].rename(
        columns={"squadHomePlayers": "players", "squadHomeId": "squadId"}
    )
    shirt_numbers_away = matches[["id", "squadAwayId", "squadAwayPlayers"]].rename(
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

    # extract substitutions
    substitutions_home = matches[["id", "squadHomeId", "squadHomeSubstitutions"]].rename(
        columns={"squadHomeSubstitutions": "squadSubstitutions", "squadHomeId": "squadId"}
    )
    substitutions_away = matches[["id", "squadAwayId", "squadAwaySubstitutions"]].rename(
        columns={"squadAwaySubstitutions": "squadSubstitutions", "squadAwayId": "squadId"}
    )

    # concat dfs
    substitutions = pd.concat([substitutions_home, substitutions_away], axis=0).reset_index(drop=True)

    # unnest formations column
    substitutions = substitutions.explode("squadSubstitutions").reset_index(drop=True)

    # drop emtpy row that occurs if one team did not substitute
    substitutions = substitutions[substitutions.squadSubstitutions.notnull()].reset_index(drop=True)

    # normalize the JSON structure into separate columns
    substitutions = substitutions.join(pd.json_normalize(substitutions["squadSubstitutions"]))

    # drop the original column
    substitutions.drop(columns=["squadSubstitutions"], inplace=True)

    # fix potential typing issues
    substitutions.exchangedPlayerId = substitutions.exchangedPlayerId.astype("Int64")

    # start merging dfs

    # merge substitutions with squads
    substitutions = substitutions.merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "squadName"}),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_x")
    )

    # merge substitutions with shirt numbers
    substitutions = substitutions.merge(
        shirt_numbers,
        left_on=["playerId", "squadId", "id"],
        right_on=["playerId", "squadId", "id"],
        how="left",
        suffixes=("", "_x")
    ).merge(
        shirt_numbers.rename(
            columns={"playerId": "exchangedPlayerId", "shirtNumber": "exchangedShirtNumber"}
        ),
        left_on=["exchangedPlayerId", "squadId", "id"],
        right_on=["exchangedPlayerId", "squadId", "id"],
        how="left",
        suffixes=("", "_x")
    )

    # merge substitutions with players
    substitutions = substitutions.merge(
        players[["id", "commonname"]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="playerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={"commonname": "exchangedPlayerName"}
        ),
        left_on="exchangedPlayerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge with matches info
    substitutions = substitutions.merge(
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
    substitutions = substitutions.merge(
        iterations[["id", "competitionName", "competitionId", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # rename some columns
    substitutions = substitutions.rename(columns={
        "id": "matchId",
        "positionSide": "toPositionSide",
        "scheduledDate": "dateTime",
        "gameTime.gameTime": "gameTime",
        "gameTime.gameTimeInSec": "gameTimeInSec"
    })

    # fix column types
    substitutions["shirtNumber"] = substitutions["shirtNumber"].astype("Int64")
    substitutions["exchangedShirtNumber"] = substitutions["exchangedShirtNumber"].astype("Int64")

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
        "gameTime",
        "gameTimeInSec",
        "substitutionType",
        "playerId",
        "playerName",
        "shirtNumber",
        "fromPosition",
        "fromPositionSide",
        "toPosition",
        "toPositionSide",
        "exchangedPlayerId",
        "exchangedPlayerName",
        "exchangedShirtNumber",
    ]

    # reorder data
    substitutions = substitutions[cols]

    # reorder rows
    substitutions = substitutions.sort_values(["matchId", "squadId", "gameTimeInSec", "playerId"])

    # return events
    return substitutions


######
#
# This function returns a pandas dataframe that contains the starting formations for a
# given match
#
######


def getStartingPositions(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:
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

    # get match info
    matches = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Iterations"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = matches[matches.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = matches[~matches.id.isin(fail_matches)]

    # raise exception if no matches remaining or report removed matches
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(matches[matches.lastCalculationDate.notnull()].iterationId.unique())

    # get squads
    squads = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get players
    players = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/players",
            method="GET"
        ).process_response(
            endpoint="Players"
        ),
            iterations),
        ignore_index=True)[["id", "commonname"]].drop_duplicates()

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # extract shirt numbers
    shirt_numbers_home = matches[["id", "squadHomeId", "squadHomePlayers"]].rename(
        columns={"squadHomePlayers": "players", "squadHomeId": "squadId"}
    )
    shirt_numbers_away = matches[["id", "squadAwayId", "squadAwayPlayers"]].rename(
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
    starting_positions_home = matches[["id", "squadHomeId", "squadHomeStartingPositions"]].rename(
        columns={"squadHomeStartingPositions": "squadStartingPositions", "squadHomeId": "squadId"}
    )
    starting_positions_away = matches[["id", "squadAwayId", "squadAwayStartingPositions"]].rename(
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
    starting_positions = starting_positions.merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "squadName"}),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_x")
    )

    # merge substitutions with players
    starting_positions = starting_positions.merge(
        players[["id", "commonname"]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="playerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

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