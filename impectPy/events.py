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


def getEvents(
        matches: list, token: str, include_kpis: bool = True,
        include_set_pieces: bool = True, session: requests.Session = requests.Session()
) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getEventsFromHost(matches, include_kpis, include_set_pieces, connection, "https://api.impect.com")

# define function
def getEventsFromHost(
        matches: list, include_kpis: bool, include_set_pieces: bool, connection: RateLimitedAPI, host: str
) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # get match info
    match_data = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Match Info"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = match_data[match_data.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in fail_matches]

    # raise exception if no matches remaining or report removed matches
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(match_data[match_data.lastCalculationDate.notnull()].iterationId.unique())

    # get match events
    events = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}/events",
            method="GET"
        ).process_response(
            endpoint="Events"
        ).assign(
            matchId=match
        ),
            matches),
        ignore_index=True)

    # account for matches without dribbles, duels or opponents tagged
    attributes = [
        "dribbleDistance",
        "dribbleType",
        "dribbleResult",
        "dribblePlayerId",
        "duelDuelType",
        "duelPlayerId",
        "opponentCoordinatesX",
        "opponentCoordinatesY",
        "opponentAdjCoordinatesX",
        "opponentAdjCoordinatesY"
    ]

    # add attribute if it doesn't exist in df
    for attribute in attributes:
        if attribute not in events.columns:
            events[attribute] = np.nan

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

    # get coaches
    coaches = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/coaches",
            method="GET"
        ).process_response(
            endpoint="Coaches",
            raise_exception=False
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

    if include_kpis:
        # get event scorings
        scorings = pd.concat(
            map(lambda match: connection.make_api_request_limited(
                url=f"{host}/v5/customerapi/matches/{match}/event-kpis",
                method="GET"
            ).process_response(
                endpoint="Scorings"
            ),
                matches),
            ignore_index=True)

        # get kpis
        kpis = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/kpis/event",
            method="GET"
        ).process_response(
            endpoint="EventKPIs"
        )[["id", "name"]]

    if include_set_pieces:
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

    # fix potential typing issues
    events.pressingPlayerId = events.pressingPlayerId.astype("Int64")
    events.fouledPlayerId = events.fouledPlayerId.astype("Int64")
    events.passReceiverPlayerId = events.passReceiverPlayerId.astype("Int64")
    events.duelPlayerId = events.duelPlayerId.astype("Int64")
    events.fouledPlayerId = events.fouledPlayerId.astype("Int64")
    if include_set_pieces:
        set_pieces.setPieceSubPhaseMainEventPlayerId = set_pieces.setPieceSubPhaseMainEventPlayerId.astype("Int64")
        set_pieces.setPieceSubPhaseFirstTouchPlayerId = set_pieces.setPieceSubPhaseFirstTouchPlayerId.astype("Int64")
        set_pieces.setPieceSubPhaseSecondTouchPlayerId = set_pieces.setPieceSubPhaseSecondTouchPlayerId.astype("Int64")

    # start merging dfs

    # merge events with secondary data
    events = events.merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "squadName"}),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    ).merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "currentAttackingSquadName"}),
        left_on="currentAttackingSquadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_away")
    ).merge(
        players[["id", "commonname"]].rename(columns={"id": "playerId", "commonname": "playerName"}),
        left_on="playerId",
        right_on="playerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={"id": "pressingPlayerId", "commonname": "pressingPlayerName"}),
        left_on="pressingPlayerId",
        right_on="pressingPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(columns={"id": "fouledPlayerId", "commonname": "fouledPlayerName"}),
        left_on="fouledPlayerId",
        right_on="fouledPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(columns={"id": "duelPlayerId", "commonname": "duelPlayerName"}),
        left_on="duelPlayerId",
        right_on="duelPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={"id": "passReceiverPlayerId", "commonname": "passReceiverPlayerName"}),
        left_on="passReceiverPlayerId",
        right_on="passReceiverPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[["id", "commonname"]].rename(
            columns={"id": "dribbleOpponentPlayerId", "commonname": "dribbleOpponentPlayerName"}),
        left_on="dribblePlayerId",
        right_on="dribbleOpponentPlayerId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        matchplan,
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        match_data[["id", "squadHomeCoachId", "squadAwayCoachId"]].rename(
            columns={"squadHomeCoachId": "homeSquadCoachId", "squadAwayCoachId": "awaySquadCoachId"}),
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        coaches[["id", "name"]].rename(columns={"id": "homeCoachId", "name": "homeCoachName"}),
        left_on="homeSquadCoachId",
        right_on="homeCoachId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        coaches[["id", "name"]].rename(columns={"id": "awayCoachId", "name": "awayCoachName"}),
        left_on="awaySquadCoachId",
        right_on="awayCoachId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        iterations,
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    if include_kpis:
        # unnest scorings and full join with kpi list to ensure all kpis are present
        scorings = scorings.merge(kpis, left_on="kpiId", right_on="id", how="outer") \
            .sort_values("kpiId") \
            .drop("kpiId", axis=1) \
            .fillna({"eventId": "", "position": "", "playerId": ""}) \
            .pivot_table(index=["eventId", "position", "playerId"], columns="name", values="value", aggfunc="sum",
                         fill_value=None) \
            .reset_index() \
            .loc[lambda df: df["eventId"].notna()]

        # Replace empty strings with None in the eventId and playerId column
        scorings["eventId"] = scorings["eventId"].mask(scorings["eventId"] == "", None)
        scorings["playerId"] = scorings["playerId"].mask(scorings["playerId"] == "", None)
        events["playerId"] = events["playerId"].mask(events["playerId"] == "", None)

        # Convert column eventId from float to int
        scorings["eventId"] = scorings["eventId"].astype(pd.Int64Dtype())
        scorings["playerId"] = scorings["playerId"].astype(pd.Int64Dtype())
        events["playerId"] = events["playerId"].astype(pd.Int64Dtype())

        # merge events and scorings
        events = events.merge(scorings,
                              left_on=["playerPosition", "playerId", "id"],
                              right_on=["position", "playerId", "eventId"],
                              how="left",
                              suffixes=("", "_scorings"))

    if include_set_pieces:
        events = events.merge(
            set_pieces,
            left_on=["setPieceId", "setPieceSubPhaseId"],
            right_on=["setPieceId", "setPieceSubPhaseId"],
            how="left",
            suffixes=("", "_right")
        ).merge(
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
    events = events.rename(columns={
        "currentAttackingSquadId": "attackingSquadId",
        "currentAttackingSquadName": "attackingSquadName",
        "duelDuelType": "duelType",
        "scheduledDate": "dateTime",
        "gameTimeGameTime": "gameTime",
        "gameTimeGameTimeInSec": "gameTimeInSec",
        "eventId": "eventId_scorings",
        "id": "eventId",
        "index": "eventNumber",
        "phaseIndex": "setPiecePhaseIndex",
        "setPieceMainEvent": "setPieceSubPhaseMainEvent",
    })

    # define desired column order
    event_cols = [
        "matchId",
        "dateTime",
        "competitionId",
        "competitionName",
        "competitionType",
        "iterationId",
        "season",
        "matchDayIndex",
        "matchDayName",
        "homeSquadId",
        "homeSquadName",
        "homeSquadCountryId",
        "homeSquadCountryName",
        "homeCoachId",
        "homeCoachName",
        "homeSquadType",
        "awaySquadId",
        "awaySquadName",
        "awaySquadCountryId",
        "awaySquadCountryName",
        "awaySquadType",
        "awayCoachId",
        "awayCoachName",
        "eventId",
        "eventNumber",
        "sequenceIndex",
        "periodId",
        "gameTime",
        "gameTimeInSec",
        "duration",
        "squadId",
        "squadName",
        "attackingSquadId",
        "attackingSquadName",
        "phase",
        "playerId",
        "playerName",
        "playerPosition",
        "playerPositionSide",
        "actionType",
        "action",
        "bodyPart",
        "bodyPartExtended",
        "previousPassHeight",
        "result",
        "startCoordinatesX",
        "startCoordinatesY",
        "startAdjCoordinatesX",
        "startAdjCoordinatesY",
        "startPackingZone",
        "startPitchPosition",
        "startLane",
        "endCoordinatesX",
        "endCoordinatesY",
        "endAdjCoordinatesX",
        "endAdjCoordinatesY",
        "endPackingZone",
        "endPitchPosition",
        "endLane",
        "opponents",
        "pressure",
        "distanceToGoal",
        "pxTTeam",
        "pxTOpponent",
        "pressingPlayerId",
        "pressingPlayerName",
        "distanceToOpponent",
        "opponentCoordinatesX",
        "opponentCoordinatesY",
        "opponentAdjCoordinatesX",
        "opponentAdjCoordinatesY",
        "passReceiverType",
        "passReceiverPlayerId",
        "passReceiverPlayerName",
        "passDistance",
        "passAngle",
        "dribbleDistance",
        "dribbleType",
        "dribbleResult",
        "dribbleOpponentPlayerId",
        "dribbleOpponentPlayerName",
        "shotDistance",
        "shotAngle",
        "shotTargetPointY",
        "shotTargetPointZ",
        "shotWoodwork",
        "shotGkCoordinatesX",
        "shotGkCoordinatesY",
        "shotGkAdjCoordinatesX",
        "shotGkAdjCoordinatesY",
        "shotGkDivePointY",
        "shotGkDivePointZ",
        "duelType",
        "duelPlayerId",
        "duelPlayerName",
        "fouledPlayerId",
        "fouledPlayerName",
        "formationTeam",
        "formationOpponent",
        "inferredSetPiece",
    ]

    set_piece_cols = [
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
        "setPieceSubPhaseMainEvent",
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
    ]

    # add columns that might not exist in previous data versions
    for col in event_cols:
        if col not in events.columns:
            events[col] = np.nan

    # create order
    order = event_cols

    if include_set_pieces:
        # add kpis
        order = order + set_piece_cols

    if include_kpis:
        # get list of kpi columns
        kpi_cols = kpis["name"].tolist()

        # add kpis
        order = order + kpi_cols

    # reorder data
    events = events[order]

    # reorder rows
    events = events.sort_values(["matchId", "eventNumber"])

    # return events
    return events