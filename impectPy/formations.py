# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost


######
#
# This function returns a pandas dataframe that contains all events for a
# given match
#
######


def getFormations(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getFormationsFromHost(matches, connection, "https://api.impect.com")


# define function
def getFormationsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
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
            "id", "skillCornerId", "heimSpielId", "wyscoutId", "matchDayIndex",
            "matchDayName", "scheduledDate", "lastCalculationDate", "iterationId"
        ]],
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge with competition info
    formations = formations.merge(
        iterations[["id", "competitionName", "competitionId", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
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