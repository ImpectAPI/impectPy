# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df, ForbiddenError, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all scores for a
# given iteration aggregated per squad
#
######


def getSquadIterationScores(iteration: int, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadIterationScoresFromHost(iteration, connection, "https://api.impect.com")

def getSquadIterationScoresFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Input for iteration argument must be an integer")

    # get squads
    squads = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
        method="GET"
    ).process_response(
        endpoint="Squads"
    )[["id", "name", "idMappings"]]

    # unnest mappings
    squads = unnest_mappings_df(squads, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get squad iteration averages
    scores_raw = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squad-scores",
        method="GET"
    ).process_response(
        endpoint="SquadIterationScores"
    ).assign(iterationId=iteration)

    # get scores
    scores_definitions = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/squad-scores",
        method="GET"
    ).process_response(
        endpoint="scoreDefinitions"
    )[["id", "name"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get matches played
    matches = scores_raw[["squadId", "matches"]].drop_duplicates()

    # unnest scores
    scores = scores_raw.explode("squadScores").reset_index(drop=True)

    # unnest dictionary in kpis column
    scores = pd.concat(
        [scores.drop(["squadScores"], axis=1), pd.json_normalize(scores["squadScores"])],
        axis=1
    )

    # merge with kpis to ensure all kpis are present
    scores = scores.merge(
        scores_definitions,
        left_on="squadScoreId",
        right_on="id",
        how="outer",
        suffixes=("", "_right")
    )

    # pivot kpi values
    scores = pd.pivot_table(
        scores,
        values="value",
        index=["iterationId", "squadId"],
        columns="name",
        aggfunc="sum",
        fill_value=0,
        dropna=False
    ).reset_index()

    # inner join with matches played
    scores = pd.merge(
        scores,
        matches,
        left_on="squadId",
        right_on="squadId",
        how="inner",
        suffixes=("", "_right")
    )

    # merge with other data
    scores = scores.merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_right")
    )

    # remove NA rows
    averages = scores[scores.iterationId.notnull()]

    # fix column types
    averages["matches"] = averages["matches"].astype("Int64")
    averages["iterationId"] = averages["iterationId"].astype("Int64")
    averages["squadId"] = averages["squadId"].astype("Int64")
    averages["wyscoutId"] = averages["wyscoutId"].astype("Int64")
    averages["heimSpielId"] = averages["heimSpielId"].astype("Int64")
    averages["skillCornerId"] = averages["skillCornerId"].astype("Int64")

    # define column order
    order = [
        "iterationId",
        "competitionName",
        "season",
        "squadId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "squadName",
        "matches"
    ]

    # add scoreNames to order
    order = order + scores_definitions.name.to_list()

    # select columns
    averages = averages[order]

    # return result
    return averages