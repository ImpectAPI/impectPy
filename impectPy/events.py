# load packages
import pandas as pd
import numpy as np
from impectPy.helpers import RateLimitedAPI
from .matches import getMatches
from .iterations import getIterations

######
#
# This function returns a pandas dataframe that contains all events for a
# given match
#
######


# define function
def getEvents(matches: list, token: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not type(matches) == list:
        raise Exception("Argument 'matches' must be a list of integers.")

    # get match events
    events = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}/events",
            method="GET",
            headers=my_header
        ).process_response(
        ).assign(
            matchId=match
        ),
            matches),
        ignore_index=True)

    # get event scorings
    scorings = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}/event-kpis",
            method="GET",
            headers=my_header
        ).process_response(),
            matches),
        ignore_index=True)

    # get match info
    iterations = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}",
            method="GET",
            headers=my_header
        ).process_response(),
            matches),
        ignore_index=True)

    # extract iterationIds
    iterations = list(iterations.iterationId.unique())

    # get players
    players = pd.concat(
        map(lambda iteration: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/players",
            method="GET",
            headers=my_header
        ).process_response(),
            iterations),
        ignore_index=True)

    # get squads
    squads = pd.concat(
        map(lambda iteration: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads",
            method="GET",
            headers=my_header
        ).process_response(),
            iterations),
        ignore_index=True)

    # get kpis
    kpis = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/kpis/event",
        method="GET",
        headers=my_header
    ).process_response()

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatches(
            iteration=iteration,
            token=token,
            session=rate_limited_api.session
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)

    # fix potential typing issues
    events.pressingPlayerId = events.pressingPlayerId.astype('Int64')
    events.fouledPlayerId = events.fouledPlayerId.astype('Int64')

    # start merging dfs

    # merge events with squads
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
    )

    # merge events with players
    events = events.merge(
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
    )

    # merge with matches info
    events = events.merge(
        matchplan,
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge with competition info
    events = events.merge(
        iterations,
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # unnest scorings and full join with kpi list to ensure all kpis are present
    scorings = scorings.merge(kpis, left_on="kpiId", right_on="id", how="outer") \
        .sort_values("kpiId") \
        .drop("kpiId", axis=1) \
        .fillna({"eventId": "", "position": "", "playerId": ""}) \
        .pivot_table(index=["eventId", "position", "playerId"], columns="name", values="value", aggfunc="sum",
                     fill_value=None) \
        .reset_index() \
        .loc[lambda df: df["eventId"].notna()]

    # Replace empty strings with NaN in the eventId and playerId column
    scorings["eventId"].replace('', np.nan, inplace=True)
    scorings["playerId"].replace('', np.nan, inplace=True)
    events["playerId"].replace('', np.nan, inplace=True)

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
        "index": "eventNumber"
    })

    # define desired column order
    attribute_cols = [
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
        "homeSquadType",
        "awaySquadId",
        "awaySquadName",
        "awaySquadCountryId",
        "awaySquadCountryName",
        "awaySquadType",
        "eventId",
        "eventNumber",
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
        "actionType",
        "action",
        "bodyPart",
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
        "passReceiverType",
        "passReceiverPlayerId",
        "passReceiverPlayerName",
        "passDistance",
        "passAngle",
        "shotDistance",
        "shotAngle",
        "shotTargetPointY",
        "shotTargetPointZ",
        "duelType",
        "duelPlayerId",
        "duelPlayerName",
        "fouledPlayerId",
        "fouledPlayerName"
    ]

    # get list of kpi columns
    kpi_cols = kpis['name'].tolist()

    # create order
    order = attribute_cols + kpi_cols

    # reorder data
    events = events[order]

    # reorder rows
    events = events.sort_values(["matchId", "eventNumber"])

    # return events
    return events