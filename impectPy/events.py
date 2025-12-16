# load packages
import numpy as np
import pandas as pd
import requests
import re
import warnings
from impectPy.helpers import RateLimitedAPI, ForbiddenError, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

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

    # get match events
    def fetch_match_events(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Match Events")

    # create list to store dfs
    events_list = []
    for match in matches:
        events = safe_execute(
            fetch_match_events,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}/events",
            identifier=f"{match}",
            forbidden_list=forbidden_matches
        ).assign(matchId=match)
        events_list.append(events)
    events = pd.concat(events_list)

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

    # get coaches
    coaches_blacklisted = False
    coaches_list = []
    for iteration in iterations:
        try:
            coaches = connection.make_api_request_limited(
                url=f"{host}/v5/customerapi/iterations/{iteration}/coaches",
                method="GET"
            ).process_response(
                endpoint="Coaches",
                raise_exception=False
            )[["id", "name"]]
            coaches_list.append(coaches)
        except KeyError:
            # no coaches found, create empty df
            coaches_list.append(pd.DataFrame(columns=["id", "name"]))
        except ForbiddenError:
            coaches_blacklisted = True
    coaches = pd.concat(coaches_list).drop_duplicates()

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

    if include_kpis:

        # get event kpis
        def fetch_event_kpis(connection, url):
            return connection.make_api_request_limited(
                url=url,
                method="GET"
            ).process_response(endpoint="Scorings")

        # create list to store dfs
        scorings_list = []
        for match in matches:
            scorings = safe_execute(
                fetch_event_kpis,
                connection,
                url=f"{host}/v5/customerapi/matches/{match}/event-kpis",
                identifier=f"{match}",
                forbidden_list=forbidden_matches
            )
            scorings_list.append(scorings)
        scorings = pd.concat(scorings_list)

        # get kpis
        kpis = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/kpis/event",
            method="GET"
        ).process_response(
            endpoint="EventKPIs"
        )[["id", "name"]]

    if include_set_pieces:

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

    # merge events with master data
    events["squadName"] = events.squadId.map(squad_map)
    events["currentAttackingSquadName"] = events.currentAttackingSquadId.map(squad_map)
    events["playerName"] = events.playerId.map(player_map)
    events["pressingPlayerName"] = events.pressingPlayerId.map(player_map)
    events["fouledPlayerName"] = events.fouledPlayerId.map(player_map)
    events["duelPlayerName"] = events.duelPlayerId.map(player_map)
    events["passReceiverPlayerName"] = events.passReceiverPlayerId.map(player_map)
    events["dribbleOpponentPlayerName"] = events.dribblePlayerId.map(player_map)
    events = events.merge(
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
        iterations,
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    if not coaches_blacklisted:

        # create coaches map
        coaches_map = coaches.set_index("id")["name"].to_dict()

        # convert coachId to integer if it is None
        events["homeSquadCoachId"] = events["homeSquadCoachId"].astype("Int64")
        events["awaySquadCoachId"] = events["awaySquadCoachId"].astype("Int64")
        events["homeSquadCoachName"] = events.homeSquadCoachId.map(coaches_map)
        events["awaySquadCoachName"] = events.awaySquadCoachId.map(coaches_map)

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
        )
        events["setPieceSubPhaseMainEventPlayerName"] = events.setPieceSubPhaseMainEventPlayerId.map(player_map)
        events["setPieceSubPhasePassReceiverName"] = events.setPieceSubPhasePassReceiverId.map(player_map)
        events["setPieceSubPhaseFirstTouchPlayerName"] = events.setPieceSubPhaseFirstTouchPlayerId.map(player_map)
        events["setPieceSubPhaseSecondTouchPlayerName"] = events.setPieceSubPhaseSecondTouchPlayerId.map(player_map)

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
        "dribblePlayerId": "dribbleOpponentPlayerId",
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

    if coaches_blacklisted:
        order = [col for col in order if col not in ["homeCoachId", "homeCoachName", "awayCoachId", "awayCoachName"]]

    # reorder data
    events = events[order]

    # reorder rows
    events = events.sort_values(["matchId", "eventNumber"])

    # return events
    return events