######
#
# This function returns an XML file from a given match event dataframe
#
######

# load packages
from xml.etree import ElementTree as ET
import pandas as pd


# define function
def generateSportsCodeXML(events: pd.DataFrame,
                lead: int,
                lag: int,
                p1Start: int,
                p2Start: int,
                p3Start: int,
                p4Start: int) -> ET.ElementTree:
    # define parameters
    ## compile periods start times into dict
    offsets = {"p1": p1Start,
               "p2": p2Start,
               "p3": p3Start,
               "p4": p4Start}

    ## define list of kpis to be included
    kpis = ["BYPASSED_OPPONENTS",
            "BYPASSED_DEFENDERS",
            "BYPASSED_OPPONENTS_RECEIVING",
            "BYPASSED_DEFENDERS_RECEIVING",
            "BALL_LOSS_ADDED_OPPONENTS",
            "BALL_LOSS_REMOVED_TEAMMATES",
            "BALL_WIN_ADDED_TEAMMATES",
            "BALL_WIN_REMOVED_OPPONENTS"]

    ## create empty dict to store bucket definitions for kpis
    kpi_buckets = {}

    ## define bucket limits
    buckets = [{"label": "[0,1[",
                "min": 0,
                "max": 1},
               {"label": "[1,3[",
                "min": 1,
                "max": 3},
               {"label": "[3,5[",
                "min": 3,
                "max": 5},
               {"label": ">=5",
                "min": 5,
                "max": 50}]

    ## iterate over kpis and add buckets to dict
    for kpi in kpis:
        kpi_buckets[kpi] = buckets

    ## define pressure buckets
    pressure_buckets = [{"label": "[0,30[",
                         "min": -1,
                         "max": 30},
                        {"label": "[30,70[",
                         "min": 30,
                         "max": 70},
                        {"label": "[70,100]",
                         "min": 70,
                         "max": 101}]

    ## define opponent buckets
    opponent_buckets = [{"label": "[0,5[",
                         "min": -1,
                         "max": 5},
                        {"label": "[5,9[",
                         "min": 5,
                         "max": 9},
                        {"label": "[9,11]",
                         "min": 9,
                         "max": 12}]

    ## define delta pxt bucket
    pxt_buckets = [{"label": "[0%,1%[",
                    "min": 0,
                    "max": 0.01},
                   {"label": "[1%,2%[",
                    "min": 0.01,
                    "max": 0.02},
                   {"label": "[2%,5%[",
                    "min": 0.02,
                    "max": 0.05},
                   {"label": "[5%,10%[",
                    "min": 0.05,
                    "max": 0.1},
                   {"label": "[10%,100%]",
                    "min": 0.1,
                    "max": 1.01},
                   {"label": "[-1%,0%[",
                    "min": -0.01,
                    "max": 0},
                   {"label": "[-2%,-1%[",
                    "min": -0.02,
                    "max": -0.01},
                   {"label": "[-5%,-2%[",
                    "min": -0.05,
                    "max": -0.02},
                   {"label": "[-10%,-5%[",
                    "min": -0.1,
                    "max": -0.05},
                   {"label": "[-100%,-10%[",
                    "min": -1.,
                    "max": -0.1}]

    ## define pass length buckets
    pass_buckets = [{"label": "<15",
                     "min": -1,
                     "max": 15},
                    {"label": ">=15",
                     "min": 15,
                     "max": 200}]

    ## define color schemes
    home_colors = {"r": "62929",
                   "g": "9225",
                   "b": "105"}

    away_colors = {"r": "13171",
                   "g": "20724",
                   "b": "40300"}

    neutral_colors = {"r": "13001",
                      "g": "13001",
                      "b": "13001"}

    # combine pxT kpis into single score for players (incl. PXT_REC) and team (excl. PXT_REC)
    events["PXT_PLAYER_DELTA"] = events.apply(
        lambda x: x.PXT_BLOCK + x.PXT_DRIBBLE + x.PXT_FOUL + x.PXT_BALL_WIN +
                  x.PXT_PASS + x.PXT_REC + x.PXT_SHOT + x.PXT_SETPIECE, axis=1)
    events["PXT_TEAM_DELTA"] = events.apply(
        lambda x: x.PXT_BLOCK + x.PXT_DRIBBLE + x.PXT_FOUL + x.PXT_BALL_WIN +
                  x.PXT_PASS + x.PXT_SHOT + x.PXT_SETPIECE, axis=1)

    # determine video timestamps

    ## define function to calculate start time
    def start_time(gameTimeInSec, periodId):
        # get period offset
        offset = offsets[f"p{periodId}"]
        # calculate and return start time
        return gameTimeInSec - (periodId - 1) * 10000 + offset - lead

    ## define function to calculate end time
    def end_time(gameTimeInSec, periodId, duration):
        # get period offset
        offset = offsets[f"p{periodId}"]
        # calculate and return end time
        return gameTimeInSec - (periodId - 1) * 10000 + offset + duration + lag

    ## apply start and end time functions
    events["start"] = events.apply(
        lambda x: start_time(x["gameTimeInSec"], x["periodId"]), axis=1)
    events["end"] = events.apply(
        lambda x: end_time(x["gameTimeInSec"], x["periodId"], x["duration"]), axis=1)

    ## fix end time for final whistles
    ## (The duration of first half final whistles is always extremely high, as it is computed using the
    ## gameTimeInSec of the FINAL_WHISTLE event (e.g. 2730) and the gameTimeInSec of the next KICKOFF event
    ## (e.g. 10000).)
    events.end = events.apply(lambda x: x.end if x.action != "FINAL_WHISTLE" else x.start + lead + lag, axis=1)

    # Group sequential plays by same player

    ## as the event data often has multiple consecutive events of the same player (e.g. reception + dribble + pass),
    ## those would be 3 separate video sequences. Because auf lead and lag times, those consecutive events would overlap
    ## significantly. TTherefore, these events are combined into one clip.

    ## copy data
    players = events.copy()

    ## create lag column for player
    players["playerId_lag"] = players.playerId.shift(1, fill_value=0)

    ## detect changes in playerId compared to previous event using lag column
    players["player_change_flag"] = players.apply(
        lambda x: 0 if x.playerId == x.playerId_lag else 1, axis=1)

    ## apply cumulative sum function to phase_change_flag to create ID column
    players["sequence_id"] = players.player_change_flag.cumsum()

    ## create separate df to aggregate sequence timing
    sequence_timing = players.copy().groupby("sequence_id").agg(
        {"start": "min",
         "end": "max"}
    ).reset_index()

    # calculate game state

    ## detect goals scored
    players["goal_home"] = players.apply(
        lambda x: 1 if (x.action == "GOAL" and x.squadId == x.squadHomeSquadId)
                       or (x.action == "OWN_GOAL" and x.squadId == x.squadAwaySquadId) else 0, axis=1)
    players["goal_away"] = players.apply(
        lambda x: 1 if (x.action == "GOAL" and x.squadId == x.squadAwaySquadId)
                       or (x.action == "OWN_GOAL" and x.squadId == x.squadHomeSquadId) else 0, axis=1)

    ## create lag column for goals because the game state should change after the goal is scored not on the
    ## goal event itself
    players["goal_home_lag"] = players.goal_home.shift(1, fill_value=0)
    players["goal_away_lag"] = players.goal_away.shift(1, fill_value=0)

    ## apply cumulative sum function to goal_home_lag and goal_away_lag
    players["goal_home_sum"] = players.goal_home_lag.cumsum()
    players["goal_away_sum"] = players.goal_away_lag.cumsum()

    ## calculate teamGoals and opponentGoals
    players["teamGoals"] = players.apply(
        lambda x: x.goal_home_sum if x.squadId == x.squadHomeSquadId else (
            x.goal_away_sum if x.squadId == x.squadAwaySquadId else None), axis=1)
    players["opponentGoals"] = players.apply(
        lambda x: x.goal_home_sum if x.squadId == x.squadAwaySquadId else (
            x.goal_away_sum if x.squadId == x.squadHomeSquadId else None), axis=1)

    ## calculate game state
    players["gameState"] = players.apply(
        lambda x: "tied" if x.teamGoals == x.opponentGoals else (
            "leading" if x.teamGoals > x.opponentGoals else ("trailing" if x.teamGoals < x.opponentGoals else None)),
        axis=1)

    # group possession phases

    ## create groups on team level for consecutive events that have the same attacking squad in order to determine
    ## whether an attacking possession phase leads to a shot or a goal

    ## create lag column for attackingSquad
    players["attackingSquadId_lag"] = players.attackingSquadId.shift(1)

    ## detect changes in attackingSquadName compared to previous event using lag column
    players["possession_change_flag"] = players.apply(
        lambda x: 0 if x.attackingSquadId == x.attackingSquadId_lag else 1, axis=1)

    ## apply cumulative sum function to possession_change_flag to create ID column
    players["possession_id"] = players.possession_change_flag.cumsum()

    ## create columns to detect shots and goal
    players["is_shot"] = players.apply(lambda x: 1 if x.actionType == "SHOT" else 0, axis=1)
    players["is_goal"] = players.apply(lambda x: 1 if x.actionType == "SHOT" and x.result == "SUCCESS" else 0, axis=1)

    ## create separate df to aggregate possession results
    possession_results = players.copy().groupby("possession_id").agg(
        {"is_shot": "sum",
         "is_goal": "sum"}
    ).reset_index()

    ## convert sum of goals/shots to boolean type
    possession_results["leadsToShot"] = possession_results.apply(lambda x: True if x.is_shot > 0 else False, axis=1)
    possession_results["leadsToGoal"] = possession_results.apply(lambda x: True if x.is_goal > 0 else False, axis=1)

    ## add possession result to players df
    players = pd.merge(players,
                       possession_results,
                       how="left",
                       left_on=["possession_id"],
                       right_on=["possession_id"])

    # group phases on team level

    ## create groups on team level for consecutive events that have the same phase and squadId in order to
    ## create team video clips

    ## create copy of data to evaluate phases
    phases = players.copy()
    phases = phases[phases.phase.notnull()]

    ## create lag columns for phase and squadId
    phases["phase_lag"] = phases.phase.shift(1)
    phases["squadId_lag"] = phases.squadId.shift(1)

    ## detect changes in either phase or squadId compared to previous event using lag columns
    phases["phase_change_flag"] = phases.apply(
        lambda x: 0 if x.phase == x.phase_lag and x.squadId == x.squadId_lag else 1, axis=1)

    ## apply cumulative sum function to phase_change_flag to create ID column
    phases["phase_id"] = phases.phase_change_flag.cumsum()

    ## create copies of pxTTeam
    phases["pxTTeamStart"] = phases.pxTTeam
    phases["pxTTeamEnd"] = phases.pxTTeam

    ## create columns to detect shots and goal
    phases["is_shot"] = phases.apply(lambda x: 1 if x.actionType == "SHOT" else 0, axis=1)
    phases["is_goal"] = phases.apply(lambda x: 1 if x.actionType == "SHOT" and x.result == "SUCCESS" else 0, axis=1)

    ## groupy by and aggregate
    phases = phases.groupby(["phase_id", "phase", "squadId", "squadName"]).agg(
        {"matchId": "min",
         "periodId": "min",
         "gameState": "first",
         "BYPASSED_OPPONENTS": "sum",
         "BYPASSED_DEFENDERS": "sum",
         "BYPASSED_OPPONENTS_RECEIVING": "sum",
         "BYPASSED_DEFENDERS_RECEIVING": "sum",
         "BALL_LOSS_ADDED_OPPONENTS": "sum",
         "BALL_LOSS_REMOVED_TEAMMATES": "sum",
         "BALL_WIN_ADDED_TEAMMATES": "sum",
         "BALL_WIN_REMOVED_OPPONENTS": "sum",
         "PXT_TEAM_DELTA": "sum",
         "pxTTeamStart": "first",
         "pxTTeamEnd": "last",
         "start": "min",
         "end": "max",
         "is_shot": "sum",
         "is_goal": "sum",
         "playerName": lambda x: set(list(x))}
    )

    ## convert sum of goals/shots to boolean type
    phases["leadsToShot"] = phases.apply(lambda x: True if x.is_shot > 0 else False, axis=1)
    phases["leadsToGoal"] = phases.apply(lambda x: True if x.is_goal > 0 else False, axis=1)

    ## reset index
    phases.reset_index(inplace=True)

    ## merge phase and squadName into one column to later pass into code tag
    phases["teamPhase"] = phases.apply(lambda x: x["squadName"] + " - " + x["phase"].replace("_", " "), axis=1)

    # get period starts

    ## filter for kick off events of each period
    kickoffs = events.copy()[
        (events.actionType == "KICK_OFF") & ((events.gameTimeInSec - (events.periodId - 1) * 10000) < 1)].reset_index()

    # apply bucket logic

    ## define function to apply bucket logic for events
    def get_bucket(bucket, value, zero_value, error_value):
        ### check if value is 0.0
        if value == 0:
            ### this is required because 0 values for kpis should be handled differently from attributes
            return zero_value
        ### iterate over bucket entries
        for entry in bucket:
            if entry["min"] <= value < entry["max"]:
                ### return bucket label
                return entry["label"]
        ### if no bucket was assigned, actively assign error_value
        return error_value

    ## apply on player level
    ## iterate over kpis
    for kpi in kpis:
        ### get bucket for column
        bucket = kpi_buckets[kpi]
        ### apply function
        players[kpi] = players[kpi].apply(lambda x: get_bucket(bucket, x, None, None))

    ## apply pressure bucket
    players.pressure = players.pressure.apply(lambda x: get_bucket(pressure_buckets, x, "[0%,10%[", None))

    ## apply opponents bucket
    players.opponents = players.opponents.apply(lambda x: get_bucket(opponent_buckets, x, "[0,5[", None))

    ## apply pass length bucket
    players.passDistance = players.passDistance.apply(lambda x: get_bucket(pass_buckets, x, "<15", None))

    ## apply pxt bucket to PXT_DELTA
    players["PXT_DELTA"] = players.PXT_PLAYER_DELTA.apply(lambda x: get_bucket(pxt_buckets, x, None, None))

    ## apply pxT Team bucket
    players.pxTTeam = players.pxTTeam.apply(lambda x: get_bucket(pxt_buckets, x, "[0%,1%[", None))

    ## apply on team level
    ## apply pxt bucket to PXT_DELTA
    phases["PXT_DELTA"] = phases.PXT_TEAM_DELTA.apply(lambda x: get_bucket(pxt_buckets, x, "[0%,1%[", None))

    ## apply pxT Team bucket
    phases.pxTTeamStart = phases.pxTTeamStart.apply(lambda x: get_bucket(pxt_buckets, x, "[0%,1%[", None))
    phases.pxTTeamEnd = phases.pxTTeamEnd.apply(lambda x: get_bucket(pxt_buckets, x, "[0%,1%[", None))

    ## iterate over kpis and apply buckets
    for kpi in kpis:
        ### get bucket for column
        bucket = kpi_buckets[kpi]
        ### apply function
        phases[kpi] = phases[kpi].apply(lambda x: get_bucket(bucket, x, None, None))

    # convert to sportscode xml

    # build a tree structure
    root = ET.Element("file")
    sort_info = ET.SubElement(root, "SORT_INFO")
    sort_info.text = ""
    sort_type = ET.SubElement(sort_info, "sort_type")
    sort_type.text = "color"
    instances = ET.SubElement(root, "ALL_INSTANCES")
    rows = ET.SubElement(root, "ROWS")

    # add kickoff events to start each period

    ## define labels
    labels = ["periodId"]

    ## add to xml structure
    for row in range(0, len(kickoffs)):
        ### add instance
        instance = ET.SubElement(instances, "instance")
        ### add event id
        event_id = ET.SubElement(instance, "ID")
        event_id.text = str(
            kickoffs.index[kickoffs.eventNumber == kickoffs.iat[row, kickoffs.columns.get_loc("eventNumber")]].tolist()[
                0])
        ### add start time
        start = ET.SubElement(instance, "start")
        start.text = str(round(kickoffs.iat[row, kickoffs.columns.get_loc("start")], 2))
        ### add end time
        end = ET.SubElement(instance, "end")
        end.text = str(round(kickoffs.iat[row, kickoffs.columns.get_loc("end")], 2))
        ### add "Start" as code
        code = ET.SubElement(instance, "code")
        code.text = "Start"
        ### add labels
        for label in labels:
            ### check for nan and None (those values should be omitted and not added as label)
            if (value := str(kickoffs.iat[row, kickoffs.columns.get_loc(label)])) not in ["None", "nan"]:
                wrapper = ET.SubElement(instance, "label")
                group = ET.SubElement(wrapper, "group")
                group.text = label
                text = ET.SubElement(wrapper, "text")
                text.text = value
            else:
                pass

    # add player data to XML structure

    ## get max id from kickoffs to ensure continuous numbering
    max_id = max(kickoffs.index.tolist())

    ## concatenate actionType and result into one column if result exists
    players["actionTypeResult"] = players.apply(lambda x: x.actionType + "_" + x.result if x.result else None, axis=1)

    ## define labels to be added
    labels = ["matchId",
              "periodId",
              "phase",
              "gameState",
              "playerDetailedPosition",
              "action",
              "actionType",
              "bodyPart",
              "actionTypeResult",
              "startPackingZone",
              "startPitchPosition",
              "startLane",
              "endPackingZone",
              "endPitchPosition",
              "endLane",
              "opponents",
              "pressure",
              "pxTTeam",
              "pressingPlayerName",
              "duelType",
              "duelPlayerName",
              "fouledPlayerName",
              "passDistance",
              "passReceiverPlayerName",
              "leadsToShot",
              "leadsToGoal",
              "PXT_DELTA",
              "BYPASSED_OPPONENTS",
              "BYPASSED_DEFENDERS",
              "BYPASSED_OPPONENTS_RECEIVING",
              "BYPASSED_DEFENDERS_RECEIVING",
              "BALL_LOSS_ADDED_OPPONENTS",
              "BALL_LOSS_REMOVED_TEAMMATES",
              "BALL_WIN_ADDED_TEAMMATES",
              "BALL_WIN_REMOVED_OPPONENTS"]

    ## add data to xml structure
    ## the idea is to still iterate over each event separately but chose between
    ## creating a new instance and appending to the existing instance
    for row in range(0, len(players)):
        ### if first iteration set seq_id_current to 1
        if row == 0:
            seq_id_current = 0
        else:
            pass

        ### get new sequence_id
        seq_id_new = players.iat[row, players.columns.get_loc("sequence_id")]

        ### check if new sequence_id or first iteration
        if seq_id_new != seq_id_current or row == 0:
            ### add instance
            instance = ET.SubElement(instances, "instance")
            ### add event id
            event_id = ET.SubElement(instance, "ID")
            event_id.text = str(players.iat[row, players.columns.get_loc("sequence_id")] + max_id)
            ### add start time
            start = ET.SubElement(instance, "start")
            start.text = str(round(sequence_timing.at[seq_id_new - 1, "start"], 2))
            ### add end time
            end = ET.SubElement(instance, "end")
            end.text = str(round(sequence_timing.at[seq_id_new - 1, "end"], 2))
            ### add player as code
            code = ET.SubElement(instance, "code")
            code.text = players.iat[row, players.columns.get_loc("playerName")]
            ### add description
            free_text = ET.SubElement(instance, "free_text")
            free_text.text = f"({players.iat[row, players.columns.get_loc('gameTime')]}) " \
                             f"{players.iat[row, players.columns.get_loc('playerName')]}: " \
                             f"{players.iat[row, players.columns.get_loc('action')].lower().replace('_', ' ')}"
        else:
            ### append current action to existing description
            free_text.text += f" | {players.iat[row, players.columns.get_loc('action')].lower().replace('_', ' ')}"

        ### add labels
        for label in labels:
            ### check for nan and None (those values should be omitted and not added as label)
            if (value := str(players.iat[row, players.columns.get_loc(label)])) not in ["None", "nan"]:
                ### get value from previous event to compare if the value remains the same (and can be omitted
                ### or if the value changed and therefore has to be added)
                try:
                    prev_value = players.at[row - 1, label]
                ### if the key doesn't exist (previous to first row), assign current value
                except KeyError:
                    prev_value = players.at[row, label]
                ### check if first event of a sequence or the value is unequal to previous row
                if seq_id_new != seq_id_current or players.at[row, label] != prev_value:
                    ### add label
                    wrapper = ET.SubElement(instance, "label")
                    group = ET.SubElement(wrapper, "group")
                    group.text = label
                    text = ET.SubElement(wrapper, "text")
                    text.text = value
            else:
                ### don't add label
                pass

        ### update current sequence_id
        seq_id_current = seq_id_new

    # add team level data

    ## define labels
    labels = ["matchId",
              "periodId",
              "gameState",
              "playerName",
              "pxTTeamStart",
              "pxTTeamEnd",
              "PXT_DELTA",
              "BYPASSED_OPPONENTS",
              "BYPASSED_DEFENDERS",
              "BYPASSED_OPPONENTS_RECEIVING",
              "BYPASSED_DEFENDERS_RECEIVING",
              "BALL_LOSS_ADDED_OPPONENTS",
              "BALL_LOSS_REMOVED_TEAMMATES",
              "BALL_WIN_ADDED_TEAMMATES",
              "BALL_WIN_REMOVED_OPPONENTS",
              "leadsToShot",
              "leadsToGoal"]

    ## update max id after adding players
    max_id += players.sequence_id.max() + 1

    ## add to xml structure
    for row in range(0, len(phases)):
        ### add instance
        instance = ET.SubElement(instances, "instance")
        ### add event id
        event_id = ET.SubElement(instance, "ID")
        event_id.text = str(phases.iat[row, phases.columns.get_loc("phase_id")] + max_id)
        ### add start time
        start = ET.SubElement(instance, "start")
        start.text = str(round(phases.iat[row, phases.columns.get_loc("start")], 2))
        ### add end time
        end = ET.SubElement(instance, "end")
        end.text = str(round(phases.iat[row, phases.columns.get_loc("end")], 2))
        ### add teamPhase as code
        code = ET.SubElement(instance, "code")
        code.text = phases.iat[row, phases.columns.get_loc("teamPhase")]
        ### add labels
        for label in labels:
            ### check for label
            if label == "playerName":
                ### for label "playerName" the list of players involved need to be unpacked
                for player in phases.iat[row, phases.columns.get_loc(label)]:
                    wrapper = ET.SubElement(instance, "label")
                    group = ET.SubElement(wrapper, "group")
                    group.text = "playerInvolved"
                    text = ET.SubElement(wrapper, "text")
                    text.text = player
            else:
                ### check for nan or None (those values should be omitted and not added as label)
                if (value := str(phases.iat[row, phases.columns.get_loc(label)])) not in ["None", "nan"]:
                    wrapper = ET.SubElement(instance, "label")
                    group = ET.SubElement(wrapper, "group")
                    group.text = label
                    text = ET.SubElement(wrapper, "text")
                    text.text = value
                else:
                    pass

    # create row order

    ## get home and away team
    home_team = players.squadHomeName.unique().tolist()[0]
    away_team = players.squadAwayName.unique().tolist()[0]

    ## get home and away team players
    home_players = sorted(
        players[players.squadName == home_team].playerName.unique(), reverse=True)
    away_players = sorted(
        players[players.squadName == away_team].playerName.unique(), reverse=True)
    ## get home and away team phases
    home_phases = sorted(
        phases[phases.squadName == home_team].teamPhase.unique(), reverse=True)
    away_phases = sorted(
        phases[phases.squadName == away_team].teamPhase.unique(), reverse=True)

    ## define function to add row entries
    def row(value, colors):
        ### add row
        row = ET.SubElement(rows, "row")
        ### add code
        code = ET.SubElement(row, "code")
        code.text = value
        ### add colors
        r = ET.SubElement(row, "R")
        r.text = colors["r"]
        g = ET.SubElement(row, "G")
        g.text = colors["g"]
        b = ET.SubElement(row, "B")
        b.text = colors["b"]

    ## apply function
    ## add entries for kickoffs for each period
    row("Start", neutral_colors)

    ## add entries for away team players
    for player in away_players:
        ### call function
        row(player, away_colors)

    ## add entries for home team players
    for player in home_players:
        ### call function
        row(player, home_colors)

    ## add entries for away team phases
    for phase in away_phases:
        ### call function
        row(phase, away_colors)

    ## add entries for home team phases
    for phase in home_phases:
        ### call function
        row(phase, home_colors)

    # wrap into ElementTree and save as XML
    tree = ET.ElementTree(root)

    # return xml tree
    return tree