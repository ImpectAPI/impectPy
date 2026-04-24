# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, resolve_matches
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost


######
#
# This function returns a pandas dataframe that contains all events for a
# given match
#
######


def getFormations(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getFormationsFromHost(matches, connection, "https://api.impect.com")


# define function
def getFormationsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
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

    # extract formations
    formations_home = match_data[["id", "squadHomeId", "squadHomeFormations"]].rename(
        columns={"squadHomeFormations": "squadFormations", "squadHomeId": "squadId"}
    )
    formations_away = match_data[["id", "squadAwayId", "squadAwayFormations"]].rename(
        columns={"squadAwayFormations": "squadFormations", "squadAwayId": "squadId"}
    )

    # concat dfs
    formations = pd.concat([formations_home, formations_away], axis=0).reset_index(drop=True)

    # unnest formations column
    formations = formations.explode("squadFormations").reset_index(drop=True)

    # normalize the JSON structure into separate columns
    formations = formations.join(pd.json_normalize(formations["squadFormations"]))

    # drop the original column
    formations.drop(columns=["squadFormations"], inplace=True)

    # start merging dfs

    # merge formations with squad data
    formations["squadName"] = formations.squadId.map(squad_map)

    # merge with matches info
    formations = formations.merge(
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
    formations = formations.merge(
        iterations[["id", "competitionName", "competitionId", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
    )

    # rename some columns
    formations = formations.rename(columns={
        "id": "matchId",
        "scheduledDate": "dateTime"
    })

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
        "formation"
    ]

    # reorder data
    formations = formations[cols]

    # reorder rows
    formations = formations.sort_values(["matchId", "squadId", "gameTimeInSec"])

    # return events
    return formations