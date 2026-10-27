# load packages
import re
from typing import Optional
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all season simulations for a given iteration
#
######


# define function
def getSeasonSimulations(
        iteration: int, date: Optional[str] = None, *, token: str, session: ImpectSession = ImpectSession()
) -> pd.DataFrame:
    """Return a DataFrame of season simulations for all squads in the given iteration.

    If ``date`` (YYYY-MM-DD) is given, only the latest simulation on or before that date is returned.
    """
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSeasonSimulationsFromHost(iteration, date, connection, "https://api.impect.com")


def getSeasonSimulationsFromHost(
        iteration: int, date: Optional[str], connection: RateLimitedAPI, host: str
) -> pd.DataFrame:
    """Fetch season simulations for the given iteration from the given host and return them as a DataFrame.

    Flattens the nested simulations structure into one row per simulation date and squad, expands
    the rank probabilities into one column per finishing position, merges competition and squad
    metadata, and sorts by date, group and squad ID. Returns an empty DataFrame when no
    simulations are available.
    """
    # check input for iteration argument
    if not isinstance(iteration, int):
        raise Exception("Argument 'iteration' must be an integer.")

    # check input for date argument
    if date is not None and (not isinstance(date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date)):
        raise Exception("Argument 'date' must be a string in the format 'YYYY-MM-DD'.")

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get squads
    squads = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        )[["id", "name", "idMappings"]]

    # unnest mappings
    squads = unnest_mappings_df(squads, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # construct url
    url = f"{host}/v5/customerapi/iterations/{iteration}/predictions/season-simulations"
    if date is not None:
        url += f"?date={date}"

    # get season simulations
    simulations_raw = connection.make_api_request_limited(
        url=url,
        method="GET"
    ).process_response(
        endpoint="Season Simulations"
    )

    # check if response is empty
    if simulations_raw.empty or not simulations_raw["predictions"].iloc[0]:
        return pd.DataFrame()

    # extract JSON from the column
    nested_data = simulations_raw["predictions"].iloc[0]

    # flatten simulations df
    simulations = []
    for entry in nested_data:
        for squad in entry["squadPredictions"]:
            row = {
                "iterationId": iteration,
                "date": entry["date"],
                "stageName": entry["stageName"],
                "groupName": squad["groupName"],
                "squadId": squad["squadId"],
                "matches": squad["matches"],
                "points": squad["points"],
                "goalsFor": squad["goalsFor"],
                "goalsAgainst": squad["goalsAgainst"],
                "pointsProjected": squad["pointsProjected"],
                "goalsForProjected": squad["goalsForProjected"],
                "goalsAgainstProjected": squad["goalsAgainstProjected"],
            }
            for rank, probability in enumerate(squad["rankProbabilities"], start=1):
                row[f"rank{rank}Probability"] = probability
            simulations.append(row)

    # convert to df
    simulations = pd.DataFrame(simulations)

    # get rank probability columns (number may differ between groups, so missing ranks are NaN)
    rank_columns = sorted(
        [col for col in simulations.columns if re.fullmatch(r"rank\d+Probability", col)],
        key=lambda col: int(re.search(r"\d+", col).group())
    )

    # merge with competition info
    simulations = simulations.merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season", "competitionGender"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
    )

    # merge with squads
    simulations = simulations.merge(
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId", "dflId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_squads")
    )

    # fix some column types
    simulations["iterationId"] = simulations["iterationId"].astype("Int64")
    simulations["competitionId"] = simulations["competitionId"].astype("Int64")
    simulations["stageName"] = simulations["stageName"].astype("string")
    simulations["groupName"] = simulations["groupName"].astype("string")
    simulations["squadId"] = simulations["squadId"].astype("Int64")
    simulations["wyscoutId"] = simulations["wyscoutId"].astype("Int64")
    simulations["heimSpielId"] = simulations["heimSpielId"].astype("Int64")
    simulations["skillCornerId"] = simulations["skillCornerId"].astype("Int64")
    simulations["optaId"] = simulations["optaId"].astype("string")
    simulations["statsPerformId"] = simulations["statsPerformId"].astype("string")
    simulations["transfermarktId"] = simulations["transfermarktId"].astype("string")
    simulations["soccerdonnaId"] = simulations["soccerdonnaId"].astype("string")
    simulations["dflId"] = simulations["dflId"].astype("string")
    simulations["matches"] = simulations["matches"].astype("Int64")
    simulations["points"] = simulations["points"].astype("Int64")
    simulations["goalsFor"] = simulations["goalsFor"].astype("Int64")
    simulations["goalsAgainst"] = simulations["goalsAgainst"].astype("Int64")
    simulations["pointsProjected"] = simulations["pointsProjected"].astype("float")
    simulations["goalsForProjected"] = simulations["goalsForProjected"].astype("float")
    simulations["goalsAgainstProjected"] = simulations["goalsAgainstProjected"].astype("float")
    simulations[rank_columns] = simulations[rank_columns].astype("float")

    # define desired column order
    order = [
        "iterationId",
        "competitionId",
        "competitionName",
        "competitionType",
        "season",
        "competitionGender",
        "date",
        "stageName",
        "groupName",
        "squadId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "optaId",
        "statsPerformId",
        "transfermarktId",
        "soccerdonnaId",
        "dflId",
        "squadName",
        "matches",
        "points",
        "goalsFor",
        "goalsAgainst",
        "pointsProjected",
        "goalsForProjected",
        "goalsAgainstProjected",
    ] + rank_columns

    # reorder data
    simulations = simulations[order]

    # reorder rows
    simulations = simulations.sort_values(["date", "groupName", "squadId"], na_position="first")

    # return simulations
    return simulations
