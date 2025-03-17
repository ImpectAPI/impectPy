# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI
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

    # get match info
    iterations = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Iterations"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = iterations[iterations.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in fail_matches]

    # raise exception if no matches remaining or report removed matches
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(iterations[iterations.lastCalculationDate.notnull()].iterationId.unique())

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

    # get set piece data
    set_pieces = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}/set-pieces",
            method="GET"
        ).process_response(
            endpoint="Set-Pieces"
        ),
            matches),
        ignore_index=True
    ).rename(
        columns={"id": "setPieceId"}
    ).explode("setPieceSubPhase", ignore_index=True)

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