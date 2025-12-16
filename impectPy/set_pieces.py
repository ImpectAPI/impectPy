# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost
import re

######
#
# This function returns a pandas dataframe that contains all set pieces for a
# given match
#
######


# define function
def getSetPieces(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSetPiecesFromHost(matches, connection, "https://api.impect.com")

def getSetPiecesFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

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

    # get set piece data
    def fetch_set_pieces(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Set-Pieces")

    # create list to store dfs
    set_pieces_list = []
    for match in matches:
        set_pieces = safe_execute(
            fetch_set_pieces,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}/set-pieces",
            identifier=f"{match}",
            forbidden_list=forbidden_matches
        ).rename(
            columns={"id": "setPieceId"}
        ).explode("setPieceSubPhase", ignore_index=True)
        set_pieces_list.append(set_pieces)
    set_pieces = pd.concat(set_pieces_list).reset_index()

    # unpack setPieceSubPhase column
    set_pieces = pd.concat(
        [
            set_pieces.drop(columns=["setPieceSubPhase"]),
            pd.json_normalize(set_pieces["setPieceSubPhase"]).add_prefix("setPieceSubPhase.")
        ],
        axis=1
    ).rename(columns=lambda x: re.sub(r"\.(.)", lambda y: y.group(1).upper(), x))

    # fix typing
    set_pieces.setPieceSubPhaseMainEventPlayerId = set_pieces.setPieceSubPhaseMainEventPlayerId.astype("Int64")
    set_pieces.setPieceSubPhaseFirstTouchPlayerId = set_pieces.setPieceSubPhaseFirstTouchPlayerId.astype("Int64")
    set_pieces.setPieceSubPhaseSecondTouchPlayerId = set_pieces.setPieceSubPhaseSecondTouchPlayerId.astype("Int64")

    # start merging dfs

    # merge events with matches
    set_pieces = set_pieces.merge(
        matchplan,
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge with competition info
    set_pieces = set_pieces.merge(
        iterations,
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # determine defending squad
    set_pieces["defendingSquadId"] = set_pieces.apply(
        lambda row: row.homeSquadId if row.squadId == row.awaySquadId else row.awaySquadId,
        axis=1
    )

    # merge events with squads
    set_pieces = set_pieces.merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "attackingSquadName"}),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    ).merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "defendingSquadName"}),
        left_on="defendingSquadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_away")
    )

    # merge events with players
    set_pieces = set_pieces.merge(
        players[["id", "commonname"]].rename(
            columns={
                "id": "setPieceSubPhaseMainEventPlayerId",
                "commonname": "setPieceSubPhaseMainEventPlayerName"
            }
        ),
        left_on="setPieceSubPhaseMainEventPlayerId",
        right_on="setPieceSubPhaseMainEventPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={
                "id": "setPieceSubPhasePassReceiverId",
                "commonname": "setPieceSubPhasePassReceiverName"
            }
        ),
        left_on="setPieceSubPhasePassReceiverId",
        right_on="setPieceSubPhasePassReceiverId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={
                "id": "setPieceSubPhaseFirstTouchPlayerId",
                "commonname": "setPieceSubPhaseFirstTouchPlayerName"
            }
        ),
        left_on="setPieceSubPhaseFirstTouchPlayerId",
        right_on="setPieceSubPhaseFirstTouchPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={
                "id": "setPieceSubPhaseSecondTouchPlayerId",
                "commonname": "setPieceSubPhaseSecondTouchPlayerName"
            }
        ),
        left_on="setPieceSubPhaseSecondTouchPlayerId",
        right_on="setPieceSubPhaseSecondTouchPlayerId",
        how="left",
        suffixes=("", "_right")
    )

    # rename some columns
    set_pieces = set_pieces.rename(columns={
        "scheduledDate": "dateTime",
        "squadId": "attackingSquadId",
        "phaseIndex": "setPiecePhaseIndex",
        "setPieceSubPhaseAggregatesSHOT_XG": "setPieceSubPhase_SHOT_XG",
        "setPieceSubPhaseAggregatesPACKING_XG": "setPieceSubPhase_PACKING_XG",
        "setPieceSubPhaseAggregatesPOSTSHOT_XG": "setPieceSubPhase_POSTSHOT_XG",
        "setPieceSubPhaseAggregatesSHOT_AT_GOAL_NUMBER": "setPieceSubPhase_SHOT_AT_GOAL_NUMBER",
        "setPieceSubPhaseAggregatesGOALS": "setPieceSubPhase_GOALS",
        "setPieceSubPhaseAggregatesPXT_POSITIVE": "setPieceSubPhase_PXT_POSITIVE",
        "setPieceSubPhaseAggregatesBYPASSED_OPPONENTS": "setPieceSubPhase_BYPASSED_OPPONENTS",
        "setPieceSubPhaseAggregatesBYPASSED_DEFENDERS": "setPieceSubPhase_BYPASSED_DEFENDERS"
    })

    # define desired column order
    order = [
        "matchId",
        "dateTime",
        "competitionName",
        "competitionId",
        "competitionType",
        "iterationId",
        "season",
        "attackingSquadId",
        "attackingSquadName",
        "defendingSquadId",
        "defendingSquadName",
        "setPieceId",
        "setPiecePhaseIndex",
        "setPieceCategory",
        "adjSetPieceCategory",
        "setPieceExecutionType",
        "setPieceSubPhaseId",
        "setPieceSubPhaseIndex",
        "setPieceSubPhaseStartZone",
        "setPieceSubPhaseCornerEndZone",
        "setPieceSubPhaseCornerType",
        "setPieceSubPhaseFreeKickEndZone",
        "setPieceSubPhaseFreeKickType",
        "setPieceSubPhaseMainEventPlayerId",
        "setPieceSubPhaseMainEventPlayerName",
        "setPieceSubPhaseMainEventOutcome",
        "setPieceSubPhasePassReceiverId",
        "setPieceSubPhasePassReceiverName",
        "setPieceSubPhaseFirstTouchPlayerId",
        "setPieceSubPhaseFirstTouchPlayerName",
        "setPieceSubPhaseFirstTouchWon",
        "setPieceSubPhaseIndirectHeader",
        "setPieceSubPhaseSecondTouchPlayerId",
        "setPieceSubPhaseSecondTouchPlayerName",
        "setPieceSubPhaseSecondTouchWon",
        "setPieceSubPhase_SHOT_XG",
        "setPieceSubPhase_PACKING_XG",
        "setPieceSubPhase_POSTSHOT_XG",
        "setPieceSubPhase_SHOT_AT_GOAL_NUMBER",
        "setPieceSubPhase_GOALS",
        "setPieceSubPhase_PXT_POSITIVE",
        "setPieceSubPhase_BYPASSED_OPPONENTS",
        "setPieceSubPhase_BYPASSED_DEFENDERS",
    ]

    # reorder data
    set_pieces = set_pieces[order]

    # reorder rows
    set_pieces = set_pieces.sort_values(["matchId", "setPiecePhaseIndex"])

    # return events
    return set_pieces