import numpy as np
import pandas as pd
import sys
from xml.etree import ElementTree as ET

######
#
# This function returns an XML file from a given match event dataframe
#
######

#define allowed KPIs, labels and codes
allowed_labels = [
    {"order": "00 | ", "name": "eventId"},
    {"order": "01 | ", "name": "matchId"},
    {"order": "02 | ", "name": "periodId"},
    {"order": "03 | ", "name": "phase"},
    {"order": "04 | ", "name": "gameState"},
    {"order": "05 | ", "name": "playerPosition"},
    {"order": "06 | ", "name": "action"},
    {"order": "07 | ", "name": "actionType"},
    {"order": "08a | ", "name": "bodyPart"},
    {"order": "08b | ", "name": "bodyPartExtended"},
    {"order": "08c | ", "name": "previousPassHeight"},
    {"order": "09 | ", "name": "actionTypeResult"},
    {"order": "10 | ", "name": "startPackingZone"},
    {"order": "11 | ", "name": "startPackingZoneGroup"},
    {"order": "12 | ", "name": "startPitchPosition"},
    {"order": "13 | ", "name": "startLane"},
    {"order": "14 | ", "name": "endPackingZone"},
    {"order": "15 | ", "name": "endPackingZoneGroup"},
    {"order": "16 | ", "name": "endPitchPosition"},
    {"order": "17 | ", "name": "endLane"},
    {"order": "18 | ", "name": "opponents"},
    {"order": "19 | ", "name": "pressure"},
    {"order": "20 | ", "name": "pxTTeam"},
    {"order": "21 | ", "name": "pressingPlayerName"},
    {"order": "22 | ", "name": "duelType"},
    {"order": "23 | ", "name": "duelPlayerName"},
    {"order": "24 | ", "name": "fouledPlayerName"},
    {"order": "25 | ", "name": "passDistance"},
    {"order": "26 | ", "name": "passReceiverPlayerName"},
    {"order": "27 | ", "name": "leadsToShot"},
    {"order": "28 | ", "name": "leadsToGoal"},
    {"order": "29 | ", "name": "squadName"},
    {"order": "30 | ", "name": "playerName"},
    {"order": "31 | ", "name": "pxTTeamStart"},
    {"order": "32 | ", "name": "pxTTeamEnd"},
]

allowed_kpis = [
    {"order": "KPI: ", "name": "PXT_DELTA"},
    {"order": "KPI: ", "name": "BYPASSED_OPPONENTS"},
    {"order": "KPI: ", "name": "BYPASSED_DEFENDERS"},
    {"order": "KPI: ", "name": "BYPASSED_OPPONENTS_RECEIVING"},
    {"order": "KPI: ", "name": "BYPASSED_DEFENDERS_RECEIVING"},
    {"order": "KPI: ", "name": "BALL_LOSS_ADDED_OPPONENTS"},
    {"order": "KPI: ", "name": "BALL_LOSS_REMOVED_TEAMMATES"},
    {"order": "KPI: ", "name": "BALL_WIN_ADDED_TEAMMATES"},
    {"order": "KPI: ", "name": "BALL_WIN_REMOVED_OPPONENTS"},
    {"order": "KPI: ", "name": "REVERSE_PLAY_ADDED_OPPONENTS"},
    {"order": "KPI: ", "name": "REVERSE_PLAY_ADDED_OPPONENTS_DEFENDERS"},
    {"order": "KPI: ", "name": "BYPASSED_OPPONENTS_RAW"},
    {"order": "KPI: ", "name": "BYPASSED_OPPONENTS_DEFENDERS_RAW"},
    {"order": "KPI: ", "name": "SHOT_XG"},
    {"order": "KPI: ", "name": "POSTSHOT_XG"},
    {"order": "KPI: ", "name": "PACKING_XG"}
]

allowed_codes = [
    "playerName",
    "squadName",
    "actionType",
    "action"
]

# define allowed label/code combinations
combinations = {
    "eventId": {"playerName": True, "team": False, "action": True, "actionType": True},
    "matchId": {"playerName": True, "team": True, "action": True, "actionType": True},
    "periodId": {"playerName": True, "team": True, "action": True, "actionType": True},
    "phase": {"playerName": True, "team": False, "action": True, "actionType": True},
    "gameState": {"playerName": True, "team": True, "action": True, "actionType": True},
    "playerPosition": {"playerName": True, "team": False, "action": True, "actionType": True},
    "action": {"playerName": True, "team": False, "action": False, "actionType": True},
    "actionType": {"playerName": True, "team": False, "action": True, "actionType": False},
    "bodyPart": {"playerName": True, "team": False, "action": True, "actionType": True},
    "bodyPartExtended": {"playerName": True, "team": False, "action": True, "actionType": True},
    "previousPassHeight": {"playerName": True, "team": False, "action": True, "actionType": True},
    "actionTypeResult": {"playerName": True, "team": False, "action": True, "actionType": True},
    "startPackingZone": {"playerName": True, "team": False, "action": True, "actionType": True},
    "startPackingZoneGroup": {"playerName": True, "team": False, "action": True, "actionType": True},
    "startPitchPosition": {"playerName": True, "team": False, "action": True, "actionType": True},
    "startLane": {"playerName": True, "team": False, "action": True, "actionType": True},
    "endPackingZone": {"playerName": True, "team": False, "action": True, "actionType": True},
    "endPackingZoneGroup": {"playerName": True, "team": False, "action": True, "actionType": True},
    "endPitchPosition": {"playerName": True, "team": False, "action": True, "actionType": True},
    "endLane": {"playerName": True, "team": False, "action": True, "actionType": True},
    "opponents": {"playerName": True, "team": False, "action": True, "actionType": True},
    "pressure": {"playerName": True, "team": False, "action": True, "actionType": True},
    "pxTTeam": {"playerName": True, "team": False, "action": True, "actionType": True},
    "pressingPlayerName": {"playerName": True, "team": False, "action": True, "actionType": True},
    "duelType": {"playerName": True, "team": False, "action": True, "actionType": True},
    "duelPlayerName": {"playerName": True, "team": False, "action": True, "actionType": True},
    "fouledPlayerName": {"playerName": True, "team": False, "action": True, "actionType": True},
    "passDistance": {"playerName": True, "team": False, "action": True, "actionType": True},
    "passReceiverPlayerName": {"playerName": True, "team": False, "action": True, "actionType": True},
    "leadsToShot": {"playerName": True, "team": True, "action": True, "actionType": True},
    "leadsToGoal": {"playerName": True, "team": True, "action": True, "actionType": True},
    "squadName": {"playerName": True, "team": False, "action": True, "actionType": True},
    "playerName": {"playerName": False, "team": True, "action": True, "actionType": True},
    "pxTTeamStart": {"playerName": False, "team": True, "action": False, "actionType": False},
    "pxTTeamEnd": {"playerName": False, "team": True, "action": False, "actionType": False}
}


# define function to generate xml
def generateXML(
        events: pd.DataFrame,
        lead: int,
        lag: int,
        p1Start: int,
        p2Start: int,
        p3Start: int,
        p4Start: int,
        p5Start: int,
        codeTag: str,
        labels=None,
        kpis=None,
        labelSorting: bool = True,
        sequencing: bool = True,
        buckets: bool = True
) -> ET.ElementTree:

    # handle kpis and labels defaults
    if labels is None:
        labels = [label["name"] for label in allowed_labels if combinations.get(label.get("name")).get(codeTag)]
    if kpis is None:
        kpis = [kpi["name"] for kpi in allowed_kpis]

    # check for invalid kpis
    invalid_kpis = [kpi for kpi in kpis if kpi not in [kpi["name"] for kpi in allowed_kpis]]
    if len(invalid_kpis) > 0:
        raise ValueError(f"Invalid KPIs: {invalid_kpis}")

    # check for invalid labels
    invalid_labels = [lbl for lbl in labels if lbl not in [label["name"] for label in allowed_labels]]
    if len(invalid_labels) > 0:
        raise ValueError(f"Invalid Labels: {invalid_labels}")

    # check for invalid code tag
    if not codeTag in allowed_codes:
        raise ValueError(f"Invalid Code: {codeTag}")

    # keep only :
    # - if KPI in kpis
    # - if Label in labels
    # - if code matches legend
    labels_and_kpis = []
    invalid_labels = []
    for label in allowed_labels:
        if label.get("name") in labels and label.get("name") != codeTag: # ensure code attribute is not repeated as a label
            if combinations.get(label.get("name")).get(codeTag):
                labels_and_kpis.append(label)
            else:
                invalid_labels.append(label.get("name"))

    if len(invalid_labels) > 0:
        raise ValueError(
            f"With the selected code ('{codeTag}') following labels are invalid:\n{', '.join(invalid_labels)}"
        )

    for kpi in allowed_kpis:
        if kpi.get("name") in kpis:
            labels_and_kpis.append(kpi)

    if labelSorting:
        labels_and_kpis = sorted(labels_and_kpis, key=lambda x: x["order"])


    # compile periods start times into dict
    offsets = {
        "p1": p1Start,
        "p2": p2Start,
        "p3": p3Start,
        "p4": p4Start,
        "p5": p5Start
    }

    # create empty dict to store bucket definitions for kpis
    kpi_buckets = {}

    # define bucket limits for kpis
    buckets_packing = [
        {"label": "[0,1[",
         "min": 0,
         "max": 1},
        {"label": "[1,3[",
         "min": 1,
         "max": 3},
        {"label": "[3,5[",
         "min": 3,
         "max": 5},
        {"label": "[5,∞]",
         "min": 5,
         "max": 50}
    ]

    bucket_shotxg = [
        {"label": "[0,0.02[",
         "min": 0,
         "max": 0.03},
        {"label": "[0.02,0.05[",
         "min": 0.03,
         "max": 0.05},
        {"label": "[0.05,0.1[",
         "min": 0.05,
         "max": 0.1},
        {"label": "[0.1,0.15[",
         "min": 0.1,
         "max": 0.15},
        {"label": "[0.15,1]",
         "min": 0.15,
         "max": 1.01}
    ]

    bucket_postshotxg = [
        {"label": "[0,0.1[",
         "min": 0,
         "max": 0.1},
        {"label": "[0.1,0.2[",
         "min": 0.1,
         "max": 0.2},
        {"label": "[0.2,0.3[",
         "min": 0.2,
         "max": 0.3},
        {"label": "[0.3,0.4[",
         "min": 0.3,
         "max": 0.4},
        {"label": "[0.4,0.5]",
         "min": 0.4,
         "max": 0.5},
        {"label": "[0.5,0.6[",
         "min": 0.5,
         "max": 0.6},
        {"label": "[0.6,0.7[",
         "min": 0.6,
         "max": 0.7},
        {"label": "[0.7,0.8[",
         "min": 0.7,
         "max": 0.8},
        {"label": "[0.8,0.9[",
         "min": 0.8,
         "max": 0.9},
        {"label": "[0.9,1]",
         "min": 0.9,
         "max": 1.01}
    ]

    bucket_packingxg = [
        {"label": "[0,0.02[",
         "min": 0,
         "max": 0.03},
        {"label": "[0.02,0.05[",
         "min": 0.03,
         "max": 0.05},
        {"label": "[0.05,0.1[",
         "min": 0.05,
         "max": 0.1},
        {"label": "[0.1,0.15[",
         "min": 0.1,
         "max": 0.15},
        {"label": "[0.15,1]",
         "min": 0.15,
         "max": 1.1}
    ]

    # define delta pxt bucket
    bucket_pxt = [
        {"label": "[0%,1%[",
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
         "max": -0.1}
    ]

    # iterate over kpis and add buckets to dict
    for kpi in kpis:
        if kpi == "SHOT_XG":
            kpi_buckets[kpi] = bucket_shotxg
        elif kpi == "POSTSHOT_XG":
            kpi_buckets[kpi] = bucket_postshotxg
        elif kpi == "PACKING_XG":
            kpi_buckets[kpi] = bucket_packingxg
        elif kpi == "PXT_DELTA":
            kpi_buckets[kpi] = bucket_pxt
        else:
            kpi_buckets[kpi] = buckets_packing

    # define pressure buckets
    pressure_buckets = [
        {"label": "[0,30[",
         "min": -1,
         "max": 30},
        {"label": "[30,70[",
         "min": 30,
         "max": 70},
        {"label": "[70,100]",
         "min": 70,
         "max": 101}
    ]

    # define opponent buckets
    opponent_buckets = [
        {"label": "[0,5[",
         "min": -1,
         "max": 5},
        {"label": "[5,9[",
         "min": 5,
         "max": 9},
        {"label": "[9,11]",
         "min": 9,
         "max": 12}
    ]

    # define pass length buckets
    pass_buckets = [
        {"label": "[0,5[",
         "min": 0,
         "max": 5},
        {"label": "[5,15[",
         "min": 5,
         "max": 15},
        {"label": "[15,25[",
         "min": 15,
         "max": 25},
        {"label": "[25,∞]",
         "min": 25,
         "max": 200}
    ]

    # define color schemes
    home_colors = {
        "r": "62929",
        "g": "9225",
        "b": "105"
    }

    away_colors = {
        "r": "13171",
        "g": "20724",
        "b": "40300"
    }

    neutral_colors = {
        "r": "13001",
        "g": "13001",
        "b": "13001"
    }

    # combine pxT kpis into single score for players (incl. PXT_REC) and team (excl. PXT_REC)
    events["PXT_PLAYER_DELTA"] = events[
        ["PXT_BLOCK", "PXT_DRIBBLE", "PXT_FOUL", "PXT_BALL_WIN", "PXT_PASS", "PXT_REC", "PXT_SHOT", "PXT_SETPIECE"]
    ].sum(axis=1)

    events["PXT_TEAM_DELTA"] = events[
        ["PXT_BLOCK", "PXT_DRIBBLE", "PXT_FOUL", "PXT_BALL_WIN", "PXT_PASS", "PXT_SHOT", "PXT_SETPIECE"]
    ].sum(axis=1)

    # add grouping for packing zones
    base_zone_groups = {
        'AM': ['AMC', 'AML', 'AMR'],
        'CB': ['CBC', 'CBL', 'CBR'],
        'CM': ['CMC', 'CML', 'CMR'],
        'DM': ['DMC', 'DML', 'DMR'],
        'FBL': ['FBL'],
        'FBR': ['FBR'],
        'GK': ['GKC', 'GKR', 'GKL'],
        'IBC': ['IBC', 'IBR', 'IBL'],
        'IBWL': ['IBWL'],
        'IBWR': ['IBWR'],
        'WL': ['WL'],
        'WR': ['WR'],
    }

    # build mapping dictionary
    zone_groups = {}
    for group, zones in base_zone_groups.items():
        for zone in zones:
            zone_groups[zone] = group
            zone_groups[f'OPP_{zone}'] = f'OPP_{group}'

    # add new columns
    events["startPackingZoneGroup"] = events["startPackingZone"].map(zone_groups).fillna(events["startPackingZone"])
    events["endPackingZoneGroup"] = events["endPackingZone"].map(zone_groups).fillna(events["endPackingZone"])

    # determine video timestamps

    # vectorize period offset lookup
    period_ids = events["periodId"]
    offsets_series = period_ids.map(lambda period_id: offsets[f"p{period_id}"])

    # Compute start and end time
    events["start"] = (events["gameTimeInSec"]
                       - (period_ids - 1) * 10000
                       + offsets_series
                       - lead).clip(lower=0)

    events["end"] = (events["gameTimeInSec"]
                     - (period_ids - 1) * 10000
                     + offsets_series
                     + events["duration"]
                     + lag)

    # fix end time for final whistles
    # (The duration of first half final whistles is always extremely high, as it is computed using the
    # gameTimeInSec of the FINAL_WHISTLE event (e.g. 2730) and the gameTimeInSec of the next KICKOFF event
    # (e.g. 10000).)
    events["end"] = np.where(
        events["action"] != "FINAL_WHISTLE",
        events["end"],
        events["start"] + lead + lag
    )

    # Group sequential plays by same player

    # as the event data often has multiple consecutive events of the same player (e.g. reception + dribble + pass),
    # those would be 3 separate video sequences. Because of lead and lag times, those consecutive events would overlap
    # significantly. TTherefore, these events are combined into one clip.

    # copy data
    players = events.copy()

    # filter for rows where playerId is given
    players = players[players.playerId.notnull()]
    if sequencing:
        # create lag column for player
        players["playerId_lag"] = players.playerId.shift(1, fill_value=0)

        # detect changes in playerId compared to previous event using lag column
        players["player_change_flag"] = np.where(
            players["playerId"] == players["playerId_lag"], 0, 1
        )

        # apply cumulative sum function to phase_change_flag to create ID column
        players["sequence_id"] = players.player_change_flag.cumsum()

        # create separate df to aggregate sequence timing
        sequence_timing = players.copy().groupby("sequence_id").agg(
            {"start": "min",
             "end": "max"}
        ).reset_index()

    # calculate game state

    # detect goals scored
    players["goal_home"] = np.where(
        ((players["action"] == "GOAL") & (players["squadId"] == players["homeSquadId"])) |
        ((players["action"] == "OWN_GOAL") & (players["squadId"] == players["awaySquadId"])),
        1,
        0
    )

    players["goal_away"] = np.where(
        ((players["action"] == "GOAL") & (players["squadId"] == players["awaySquadId"])) |
        ((players["action"] == "OWN_GOAL") & (players["squadId"] == players["homeSquadId"])),
        1,
        0
    )

    # create lag column for goals because the game state should change after the goal is scored not on the
    # goal event itself
    players["goal_home_lag"] = players.goal_home.shift(1, fill_value=0)
    players["goal_away_lag"] = players.goal_away.shift(1, fill_value=0)

    # apply cumulative sum function to goal_home_lag and goal_away_lag
    players["goal_home_sum"] = players.goal_home_lag.cumsum()
    players["goal_away_sum"] = players.goal_away_lag.cumsum()

    # calculate teamGoals and opponentGoals
    players["teamGoals"] = np.where(
        players["squadId"] == players["homeSquadId"],
        players["goal_home_sum"],
        np.where(
            players["squadId"] == players["awaySquadId"],
            players["goal_away_sum"],
            np.nan
        )
    )

    players["opponentGoals"] = np.where(
        players["squadId"] == players["awaySquadId"],
        players["goal_home_sum"],
        np.where(
            players["squadId"] == players["homeSquadId"],
            players["goal_away_sum"],
            np.nan
        )
    )

    # calculate game state
    players["gameState"] = np.where(
        players["teamGoals"] == players["opponentGoals"], "tied",
        np.where(
            players["teamGoals"] > players["opponentGoals"], "leading",
            np.where(
                players["teamGoals"] < players["opponentGoals"], "trailing",
                np.NaN
            )
        )
    )

    # group possession phases

    # create groups on team level for consecutive events that have the same attacking squad in order to determine
    # whether an attacking possession phase leads to a shot or a goal

    # create lag column for attackingSquad
    players["attackingSquadId_lag"] = players.attackingSquadId.shift(1)

    # detect changes in attackingSquadName compared to previous event using lag column
    players["possession_change_flag"] = np.where(
        players["attackingSquadId"] == players["attackingSquadId_lag"], 0, 1
    )

    # apply cumulative sum function to possession_change_flag to create ID column
    players["possession_id"] = players.possession_change_flag.cumsum()

    # create columns to detect shots and goal
    players["is_shot"] = np.where(players["actionType"] == "SHOT", 1, 0)
    players["is_goal"] = np.where(
        (players["actionType"] == "SHOT") & (players["result"] == "SUCCESS"), 1, 0
    )

    # create separate df to aggregate possession results
    possession_results = players.copy().groupby("possession_id").agg(
        {"is_shot": "sum",
         "is_goal": "sum"}
    )

    # convert sum of goals/shots to boolean type
    possession_results["leadsToShot"] = possession_results["is_shot"] > 0
    possession_results["leadsToGoal"] = possession_results["is_goal"] > 0

    # add possession result to players df
    players = pd.merge(
        players,
        possession_results,
        how="left",
        left_on=["possession_id"],
        right_on=["possession_id"]
    )

    # group phases on team level

    # create groups on team level for consecutive events that have the same phase and squadId in order to
    # create team video clips

    # create copy of data to evaluate phases
    phases = players.copy()
    phases = phases[phases.phase.notnull()]

    # create lag columns for phase and squadId
    phases["phase_lag"] = phases.phase.shift(1)
    phases["squadId_lag"] = phases.squadId.shift(1)

    # detect changes in either phase or squadId compared to previous event using lag columns
    phases["phase_change_flag"] = np.where(
        (phases["phase"] == phases["phase_lag"]) & (phases["squadId"] == phases["squadId_lag"]),
        0,
        1
    )

    # apply cumulative sum function to phase_change_flag to create ID column
    phases["phase_id"] = phases.phase_change_flag.cumsum()

    # create copies of pxTTeam
    phases["pxTTeamStart"] = phases.pxTTeam
    phases["pxTTeamEnd"] = phases.pxTTeam

    # create columns to detect shots and goal
    phases["is_shot"] = np.where(phases["actionType"] == "SHOT", 1, 0)
    phases["is_goal"] = np.where(
        (phases["actionType"] == "SHOT") & (phases["result"] == "SUCCESS"), 1, 0
    )

    # groupy by and aggregate
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
         "REVERSE_PLAY_ADDED_OPPONENTS": "sum",
         "REVERSE_PLAY_ADDED_OPPONENTS_DEFENDERS": "sum",
         "BYPASSED_OPPONENTS_RAW": "sum",
         "BYPASSED_OPPONENTS_DEFENDERS_RAW": "sum",
         "SHOT_XG": "sum",
         "POSTSHOT_XG": "sum",
         "PACKING_XG": "sum",
         "PXT_TEAM_DELTA": "sum",
         "pxTTeamStart": "first",
         "pxTTeamEnd": "last",
         "start": "min",
         "end": "max",
         "is_shot": "sum",
         "is_goal": "sum",
         "playerName": lambda x: set(list(x))}
    )

    # convert sum of goals/shots to boolean type
    phases["leadsToShot"] = phases["is_shot"] > 0
    phases["leadsToGoal"] = phases["is_goal"] > 0

    # reset index
    phases.reset_index(inplace=True)

    # merge phase and squadName into one column to later pass into code tag
    phases["teamPhase"] = phases["squadName"] + " - " + phases["phase"].str.replace("_", " ")

    # get period starts

    # filter for kick off events of each period
    kickoffs = events.copy()[
        (events.actionType == "KICK_OFF") & ((events.gameTimeInSec - (events.periodId - 1) * 10000) < 10)
    ].reset_index()

    # check for penalty shootout
    penalty_shootout = events.copy()[
        events.periodId == 5
    ]

    # add row for start of penalty shootout
    if len(penalty_shootout) > 0:
        kickoffs = pd.concat([kickoffs, penalty_shootout.iloc[[0]]])

    # rename PXT_DELTA
    players = players.rename(columns={"PXT_PLAYER_DELTA": "PXT_DELTA"})
    phases = phases.rename(columns={"PXT_TEAM_DELTA": "PXT_DELTA"})

    # apply bucket logic
    if buckets:
        # define function to apply bucket logic for events
        def get_bucket(bucket, value, zero_value, error_value):
            # check if value is 0.0
            if value == 0:
                # this is required because 0 values for kpis should be handled differently from attributes
                return zero_value
            # iterate over bucket entries
            for entry in bucket:
                if entry["min"] <= value < entry["max"]:
                    # return bucket label
                    return entry["label"]
            # if no bucket was assigned, actively assign error_value
            return error_value

        # apply on player level
        # iterate over kpis
        for kpi in kpis:
            # get bucket for column
            bucket = kpi_buckets[kpi]
            # apply function
            players[kpi] = players[kpi].apply(lambda x: get_bucket(bucket, x, None, None))

        # apply pressure bucket
        players.pressure = players.pressure.apply(lambda x: get_bucket(pressure_buckets, x, "[0%,10%[", None))

        # apply opponents bucket
        players.opponents = players.opponents.apply(lambda x: get_bucket(opponent_buckets, x, "[0,5[", None))

        # apply pass length bucket
        players.passDistance = players.passDistance.apply(lambda x: get_bucket(pass_buckets, x, "<15", None))

        # apply pxT Team bucket
        players.pxTTeam = players.pxTTeam.apply(lambda x: get_bucket(bucket_pxt, x, "[0%,1%[", None))

        # apply on team level
        # apply pxt bucket to PXT_DELTA
        phases.PXT_DELTA = phases.PXT_DELTA.apply(lambda x: get_bucket(bucket_pxt, x, "[0%,1%[", None))

        # apply pxT Team bucket
        phases.pxTTeamStart = phases.pxTTeamStart.apply(lambda x: get_bucket(bucket_pxt, x, "[0%,1%[", None))
        phases.pxTTeamEnd = phases.pxTTeamEnd.apply(lambda x: get_bucket(bucket_pxt, x, "[0%,1%[", None))

        # iterate over kpis and apply buckets
        for kpi in kpis:
            if kpi == "PXT_DELTA":
                continue
            # get bucket for column
            bucket = kpi_buckets[kpi]
            # apply function
            phases[kpi] = phases[kpi].apply(lambda x: get_bucket(bucket, x, None, None))

    # convert to xml

    # build a tree structure
    root = ET.Element("file")
    sort_info = ET.SubElement(root, "SORT_INFO")
    sort_info.text = ""
    sort_type = ET.SubElement(sort_info, "sort_type")
    sort_type.text = "color"
    instances = ET.SubElement(root, "ALL_INSTANCES")
    rows = ET.SubElement(root, "ROWS")

    # add kickoff events to start each period

    # add to xml structure
    for index, event in kickoffs.iterrows():
        # add instance
        instance = ET.SubElement(instances, "instance")
        # add event id
        event_id = ET.SubElement(instance, "ID")
        event_id.text = str(event.periodId - 1)
        # add start time
        start = ET.SubElement(instance, "start")
        start.text = str(round(event.start, 2))
        # add end time
        end = ET.SubElement(instance, "end")
        end.text = str(round(event.end, 2))
        # add "Start" as code
        code = ET.SubElement(instance, "code")
        if event.periodId == 1:
            code.text = f"Kickoff"
        elif event.periodId == 2:
            code.text = f"2nd Half Kickoff"
        elif event.periodId == 3:
            code.text = f"ET Kickoff"
        elif event.periodId == 4:
            code.text = f"ET 2nd Half Kickoff"
        elif event.periodId == 5:
            code.text = f"Penalty Shootout"
        # add period label
        wrapper = ET.SubElement(instance, "label")
        group = ET.SubElement(wrapper, "group")
        group.text = "02 | periodId"
        text = ET.SubElement(wrapper, "text")
        text.text = str(event.periodId)

    # add player data to XML structure

    # get max id from kickoffs to ensure continuous numbering
    max_id = max(kickoffs.periodId.tolist()) - 1

    # concatenate actionType and result into one column if result exists
    players["actionTypeResult"] = np.where(
        players["result"].notna(),
        players["actionType"] + "_" + players["result"],
        np.NaN
    )

    # add data to xml structure
    # the idea is to still iterate over each event separately but chose between
    # creating a new instance and appending to the existing instance
    if sequencing:
        seq_id_current = None

        # If the selected code attribute is "squadName", generate XML entries from the `phases` DataFrame
        if codeTag == "squadName":
            for index, phase in phases.iterrows():
                # Create a new XML instance for each team phase
                instance = ET.SubElement(instances, "instance")

                # Set unique ID using phase_id offset by max_id
                event_id = ET.SubElement(instance, "ID")
                event_id.text = str(phase.phase_id + max_id)

                # Define the time range of the instance
                start = ET.SubElement(instance, "start")
                start.text = str(round(phase.start, 2))
                end = ET.SubElement(instance, "end")
                end.text = str(round(phase.end, 2))

                # Set the code to the team phase
                code = ET.SubElement(instance, "code")
                code.text = phase.teamPhase

                # Add labels to the instance
                for label in labels_and_kpis:
                    if label["name"] not in phase:
                        continue
                    value = str(phase[label["name"]])
                    if value not in ["None", "nan"]:
                        wrapper = ET.SubElement(instance, "label")
                        group = ET.SubElement(wrapper, "group")
                        group.text = label["order"] + label["name"] if labelSorting else label["name"]
                        text = ET.SubElement(wrapper, "text")
                        text.text = value

        # If not team-level, use player-level data from `players` DataFrame
        else:
            for index, event in players.iterrows():
                # Skip entries without valid player name
                if pd.notnull(event.playerName):
                    # Set first sequence_id
                    if index == 0:
                        seq_id_current = 0

                    seq_id_new = event.sequence_id

                    # Start new clip if new sequence or first event
                    if seq_id_new != seq_id_current or index == 0:
                        instance = ET.SubElement(instances, "instance")

                        event_id = ET.SubElement(instance, "ID")
                        event_id.text = str(event.sequence_id + max_id)

                        start = ET.SubElement(instance, "start")
                        start.text = str(round(sequence_timing.at[seq_id_new - 1, "start"], 2))
                        end = ET.SubElement(instance, "end")
                        end.text = str(round(sequence_timing.at[seq_id_new - 1, "end"], 2))

                        # Use selected attribute (e.g., playerName, action) as the main code
                        code = ET.SubElement(instance, "code")
                        code.text = str(event[codeTag])

                        # Free-text description showing action sequence
                        free_text = ET.SubElement(instance, "free_text")
                        free_text.text = f"({event.gameTime}) {event.playerName}: {event.action.lower().replace('_', ' ')}"
                    else:
                        # Append to existing free-text if still same sequence
                        free_text.text += f" | {event.action.lower().replace('_', ' ')}"

                    # Add labels to the instance
                    for label in labels_and_kpis:
                        if label["name"] not in event:
                            continue
                        value = str(event[label["name"]])
                        if value not in ["None", "nan"]:
                            try:
                                prev_value = str(players.at[index - 1, label["name"]])
                            except KeyError:
                                prev_value = value
                            # Only add label if it changed or is the first event of the sequence
                            if seq_id_new != seq_id_current or value != prev_value:
                                wrapper = ET.SubElement(instance, "label")
                                group = ET.SubElement(wrapper, "group")
                                group.text = label["order"] + label["name"] if labelSorting else label["name"]
                                text = ET.SubElement(wrapper, "text")
                                text.text = value

                    # Update current sequence ID
                    seq_id_current = seq_id_new
    else:
        # Same logic as above, but without sequencing (i.e., one clip per row)
        if codeTag == "squadName":
            for index, phase in phases.iterrows():
                instance = ET.SubElement(instances, "instance")
                event_id = ET.SubElement(instance, "ID")
                event_id.text = str(phase.phase_id + max_id)
                start = ET.SubElement(instance, "start")
                start.text = str(round(phase.start, 2))
                end = ET.SubElement(instance, "end")
                end.text = str(round(phase.end, 2))
                code = ET.SubElement(instance, "code")
                code.text = phase.teamPhase

                for label in labels_and_kpis:
                    if label["name"] not in phase:
                        continue
                    value = str(phase[label["name"]])
                    if value not in ["None", "nan"]:
                        wrapper = ET.SubElement(instance, "label")
                        group = ET.SubElement(wrapper, "group")
                        group.text = label["order"] + label["name"] if labelSorting else label["name"]
                        text = ET.SubElement(wrapper, "text")
                        text.text = value
        else:
            for index, event in players.iterrows():
                if pd.notnull(event.playerName):
                    instance = ET.SubElement(instances, "instance")
                    event_id = ET.SubElement(instance, "ID")
                    event_id.text = str(event.eventNumber + max_id)
                    start = ET.SubElement(instance, "start")
                    start.text = str(round(event.start, 2))
                    end = ET.SubElement(instance, "end")
                    end.text = str(round(event.end, 2))
                    code = ET.SubElement(instance, "code")
                    code.text = str(event[codeTag])
                    free_text = ET.SubElement(instance, "free_text")
                    free_text.text = f"({event.gameTime}) {event.playerName}: {event.action.lower().replace('_', ' ')}"

                    for label in labels_and_kpis:
                        if label["name"] not in event:
                            continue
                        value = str(event[label["name"]])
                        if value not in ["None", "nan"]:
                            wrapper = ET.SubElement(instance, "label")
                            group = ET.SubElement(wrapper, "group")
                            group.text = label["order"] + label["name"] if labelSorting else label["name"]
                            text = ET.SubElement(wrapper, "text")
                            text.text = value

    # create row order

    # get home and away team
    home_team = players.homeSquadName.unique().tolist()[0]
    away_team = players.awaySquadName.unique().tolist()[0]

    # get home and away team players
    home_players = sorted(
        players[players.squadName == home_team].playerName.unique(), reverse=True)
    away_players = sorted(
        players[players.squadName == away_team].playerName.unique(), reverse=True)
    # get home and away team phases
    home_phases = sorted(
        phases[phases.squadName == home_team].teamPhase.unique(), reverse=True)
    away_phases = sorted(
        phases[phases.squadName == away_team].teamPhase.unique(), reverse=True)

    # define function to add row entries
    def row(value, colors):
        # add row
        row = ET.SubElement(rows, "row")
        # add code
        code = ET.SubElement(row, "code")
        code.text = value
        # add colors
        r = ET.SubElement(row, "R")
        r.text = colors["r"]
        g = ET.SubElement(row, "G")
        g.text = colors["g"]
        b = ET.SubElement(row, "B")
        b.text = colors["b"]

    # apply function
    # add entries for kickoffs for each period
    row("Start", neutral_colors)

    if codeTag == "playerName":
        # add entries for away team players
        for player in away_players:
            # call function
            row(player, away_colors)

        # add entries for home team players
        for player in home_players:
            # call function
            row(player, home_colors)

    elif codeTag == "squadName":
        # add entries for away team phases
        for phase in away_phases:
            # call function
            row(phase, away_colors)

        # add entries for home team phases
        for phase in home_phases:
            # call function
            row(phase, home_colors)

    # wrap into ElementTree and save as XML
    tree = ET.ElementTree(root)

    # only apply indent if Python version >= 3.9
    if sys.version_info >= (3, 9):
        ET.indent(tree, space="  ")

    # return xml tree
    return tree