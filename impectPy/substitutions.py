# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI, ImpectSession, resolve_matches
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all substitutions for a
# given match
#
######


def getSubstitutions(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSubstitutionsFromHost(matches, connection, "https://api.impect.com")


# define function
def getSubstitutionsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    resolved = resolve_matches(matches, connection, host)
    match_data = resolved.match_data
    matches = resolved.matches
    iterations = resolved.iterations

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

    # extract substitutions
    substitutions_home = match_data[["id", "squadHomeId", "squadHomeSubstitutions"]].rename(
        columns={"squadHomeSubstitutions": "squadSubstitutions", "squadHomeId": "squadId"}
    )
    substitutions_away = match_data[["id", "squadAwayId", "squadAwaySubstitutions"]].rename(
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

    # merge substitutions with master data
    substitutions["squadName"] = substitutions.squadId.map(squad_map)
    substitutions["playerName"] = substitutions.playerId.map(player_map)
    substitutions["exchangedPlayerName"] = substitutions.exchangedPlayerId.map(player_map)

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

    # merge with matches info
    substitutions = substitutions.merge(
        matchplan[[
            "id", "skillCornerId", "heimSpielId", "wyscoutId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId", "matchDayIndex",
            "matchDayName", "scheduledDate", "lastCalculationDate", "iterationId"
        ]],
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_matchplan")
    )

    # merge with competition info
    substitutions = substitutions.merge(
        iterations[["id", "competitionName", "competitionId", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
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