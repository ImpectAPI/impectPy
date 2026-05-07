# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, safe_execute, resolve_matches
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
def getSetPieces(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of all set-piece sub-phases for the given list of match IDs."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSetPiecesFromHost(matches, connection, "https://api.impect.com")

def getSetPiecesFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch set-piece sub-phases for the given matches from the given host and return them as a DataFrame.

    Resolves match metadata, squad names, player names, and KPI aggregates from the API and
    merges them into the result, sorted by match and set-piece phase index.
    """
    resolved = resolve_matches(matches, connection, host)
    match_data = resolved.match_data
    matches = resolved.matches
    iterations = resolved.iterations
    forbidden_matches = []

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
        suffixes=("", "_matchplan")
    )

    # merge with competition info
    set_pieces = set_pieces.merge(
        iterations,
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
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
        suffixes=("", "_main")
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
        suffixes=("", "_receiver")
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
        suffixes=("", "_first")
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
        suffixes=("", "_second")
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
        "startTime",
        "startTimeInSec",
        "endTime",
        "endTimeInSec",
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
        "setPieceSubPhaseGoalKickEndZone",
        "setPieceSubPhaseGoalKickType",
        "setPieceSubPhaseThrowInEndZone",
        "setPieceSubPhaseThrowInType",
        "setPieceSubPhaseSecondDeliveryEndZone",
        "setPieceSubPhaseSecondDeliveryType",
        "setPieceSubPhaseMainEventPlayerId",
        "setPieceSubPhaseMainEventPlayerName",
        "setPieceSubPhaseMainEventOutcome",
        "setPieceSubPhasePassReceiverId",
        "setPieceSubPhasePassReceiverName",
        "setPieceSubPhaseBallTrajectory",
        "setPieceSubPhaseFirstTouchPlayerId",
        "setPieceSubPhaseFirstTouchPlayerName",
        "setPieceSubPhaseFirstTouchWon",
        "setPieceSubPhaseIndirectHeader",
        "setPieceSubPhaseSecondTouchPlayerId",
        "setPieceSubPhaseSecondTouchPlayerName",
        "setPieceSubPhaseSecondTouchWon",
        "setPieceSubPhaseSecondTouchEndZone",
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