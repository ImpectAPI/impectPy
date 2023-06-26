######
#
# This function returns a pandas dataframe that contains all events for a
# given match
#
######

# load packages
import requests
import pandas as pd
import re
import numpy as np
import time
from .helpers import make_api_request


# define function
def getEventData(match: str, token: str) -> pd.DataFrame:
    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # create session object
    with requests.Session() as session:
        # get match events
        response = make_api_request(url=f"https://api.impect.com/v4/customerapi/matches/{match}/events",
                                    method="GET",
                                    headers=my_header,
                                    session=session)

        # check response status
        response.raise_for_status()

        # get data from response
        response_data = response.json()["data"]

        # add matchId to each event and append to events list
        events = [{**event, "matchId": response_data["matchId"]} for event in response_data["events"]]

        # get match data
        response = make_api_request(url=f"https://api.impect.com/v4/customerapi/matches/{match}",
                                    method="GET",
                                    headers=my_header,
                                    session=session)

        # check response status
        response.raise_for_status()

        # get data from response
        response_data = response.json()["data"]

        # create player list
        players = {player["playerId"]: player["commonname"]
                   for side in ["squadHome", "squadAway"]
                   for squad in [response_data[side]]
                   for player in squad["players"]}

        # get competition info for match
        match_info = {key: response_data["competition"].get(key) for key in [
            "competition", "competitionId", "competitionType",
            "competitionIterationId", "competitionIterationName",
            "competitionIterationStepId", "competitionIterationStepName"]}

        # add basic match info
        match_info.update({key: response_data.get(key) for key in ["matchId", "date", "dateTime"]})

        # iterate over sides
        for side in ["squadHome", "squadAway"]:
            # add squad info
            match_info.update({f'{side}{key[0].upper()}{key[1:]}': response_data.get(side).get(key)
                               for key in ['squadId', 'name', 'squadholderType']})

            match_info.update({f'{side}Country{key[0].upper()}{key[1:]}': response_data.get(side)['country'].get(key)
                               for key in ['id', 'name']})

        # convert to pandas df
        events = pd.json_normalize(events)

        # fix column names using regex
        events = events.rename(columns=lambda x: re.sub("\.(.)", lambda y: y.group(1).upper(), x))

        # drop columns that where nested but now hold only NA values
        drop_cols = ["player", "start", "end", "pass", "shot", "duel", "pxT"]

        # create list of columns containing player ids for which names are required
        player_id_cols = ["playerId", "duelPlayerId", "pressingPlayerId", "fouledPlayerId", "passReceiverPlayerId"]

        # use list comprehension and map() to add playerName columns to events dataframe
        events[[f"{col[:-2]}Name" for col in player_id_cols]] = events[player_id_cols].applymap(
            lambda x: players.get(x))

        # add the values from the match_info dictionary as new columns in the events dataframe
        for key, value in match_info.items():
            events[key] = pd.Series([value] * len(events))

        # add squadName
        home_mask = events["squadHomeSquadId"] == events["squadId"]
        away_mask = events["squadAwaySquadId"] == events["squadId"]
        squad_name = np.where(home_mask, events["squadHomeName"],
                              np.where(away_mask, events["squadAwayName"], None))
        events["squadName"] = squad_name

        # add currentAttackingSquadName
        home_mask = events["squadHomeSquadId"] == events["currentAttackingSquadId"]
        away_mask = events["squadAwaySquadId"] == events["currentAttackingSquadId"]
        attacking_squad_name = np.where(home_mask, events["squadHomeName"],
                                        np.where(away_mask, events["squadAwayName"], None))
        events["currentAttackingSquadName"] = attacking_squad_name

        # get kpi list
        response = make_api_request(url=f"https://api.impect.com/v4/customerapi/kpis",
                                    method="GET",
                                    headers=my_header,
                                    session=session)

        # check response status
        response.raise_for_status()

        # get data from response
        kpis = response.json()["data"]

        # extract kpiIds
        kpi_ids = [kpi["kpiId"] for kpi in kpis]

        # create dictionary to store sums for each kpiId
        kpi_sums = {kpi: [] for kpi in kpi_ids}

        # iterate over rows and update kpi_sums
        for row in events.itertuples():
            # initialize dict with 0 for each kpiId
            row_sums = {kpi: 0 for kpi in kpi_ids}

            # sum values for each kpiId
            for score in row.scorings:
                kpi_id = score["kpiId"]
                if kpi_id in kpi_ids:
                    row_sums[kpi_id] += score["value"]

            # add row_sums to kpi_sums
            for kpi in kpi_ids:
                kpi_sums[kpi].append(row_sums[kpi])

        # create new DataFrame from kpi_sums and merge dfs
        events = pd.concat([events, pd.DataFrame(kpi_sums)], axis=1)

        # add scorings to drop_cols list
        drop_cols.append("scorings")

        # remove original columns
        events = events.drop(drop_cols, axis=1)

        # get dict as input for rename method
        names_map = {kpi["kpiId"]: kpi["kpiName"] for kpi in kpis}

        # add some columns to be renamed
        names_map["currentAttackingSquadId"] = "attackingSquadId"
        names_map["currentAttackingSquadName"] = "attackingSquadName"
        names_map["playerPositionPosition"] = "playerPosition"
        names_map["playerPositionDetailedPosition"] = "playerDetailedPosition"
        names_map["duelDuelType"] = "duelType"

        # replace column headers
        events = events.rename(columns=names_map)

        # define desired column oder
        attribute_cols = ["matchId",
                          "date",
                          "dateTime",
                          "competition",
                          "competitionId",
                          "competitionType",
                          "competitionIterationId",
                          "competitionIterationName",
                          "competitionIterationStepId",
                          "competitionIterationStepName",
                          "squadHomeSquadId",
                          "squadHomeName",
                          "squadHomeCountryId",
                          "squadHomeCountryName",
                          "squadHomeSquadholderType",
                          "squadAwaySquadId",
                          "squadAwayName",
                          "squadAwayCountryId",
                          "squadAwayCountryName",
                          "squadAwaySquadholderType",
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
                          "playerDetailedPosition",
                          "actionType",
                          "action",
                          "bodyPart",
                          "result",
                          'startCoordinatesX',
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
                          "fouledPlayerName"]

        # get list of kpi columns
        kpi_cols = events.columns[events.columns.get_loc("BYPASSED_OPPONENTS"):].tolist()

        # create order
        order = attribute_cols + kpi_cols

        # reorder data
        events = events[order]

        return events