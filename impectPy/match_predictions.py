# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession
from .iterations import getIterationsFromHost
from .matches import getMatchesFromHost

######
#
# This function returns a pandas dataframe that contains all match predictions for a given iteration
#
######


# define function
def getMatchPredictions(iteration: int, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of match predictions for all matches in the given iteration."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getMatchPredictionsFromHost(iteration, connection, "https://api.impect.com")


def getMatchPredictionsFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch match predictions for the given iteration from the given host and return them as a DataFrame.

    Merges prediction values (market, model, expert) with match schedules and competition metadata,
    sorted by match day and match ID.
    """
    # check input for iteration argument
    if not isinstance(iteration, int):
        raise Exception("Argument 'iteration' must be an integer.")

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get match predictions
    predictions = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/predictions/match-predictions",
        method="GET"
    ).process_response(
        endpoint="Match Predictions"
    )

    # add iteration id
    predictions["iterationId"] = iteration

    # get matches
    matches = getMatchesFromHost(iteration=iteration, connection=connection, host=host)

    # merge predictions with match info
    predictions = predictions.merge(
        matches[[
            "id",
            "matchDayIndex",
            "matchDayName",
            "homeSquadId",
            "homeSquadName",
            "awaySquadId",
            "awaySquadName",
            "scheduledDate"
        ]],
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_matches")
    ).drop(columns=["id"])

    # merge with competition info
    predictions = predictions.merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season", "competitionGender"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
    ).drop(columns=["id"])

    # fix column types
    predictions["iterationId"] = predictions["iterationId"].astype("Int64")
    predictions["competitionId"] = predictions["competitionId"].astype("Int64")
    predictions["matchId"] = predictions["matchId"].astype("Int64")
    predictions["homeSquadId"] = predictions["homeSquadId"].astype("Int64")
    predictions["awaySquadId"] = predictions["awaySquadId"].astype("Int64")
    predictions["predMarketHome"] = predictions["predMarketHome"].astype("float")
    predictions["predMarketAway"] = predictions["predMarketAway"].astype("float")
    predictions["predModelHome"] = predictions["predModelHome"].astype("float")
    predictions["predModelAway"] = predictions["predModelAway"].astype("float")
    predictions["predExpertHome"] = predictions["predExpertHome"].astype("float")
    predictions["predExpertAway"] = predictions["predExpertAway"].astype("float")

    # define desired column order
    order = [
        "iterationId",
        "competitionId",
        "competitionName",
        "competitionType",
        "season",
        "competitionGender",
        "matchId",
        "matchDayIndex",
        "matchDayName",
        "scheduledDate",
        "homeSquadId",
        "homeSquadName",
        "awaySquadId",
        "awaySquadName",
        "predMarketHome",
        "predMarketAway",
        "predModelHome",
        "predModelAway",
        "predExpertHome",
        "predExpertAway"
    ]

    # reorder data
    predictions = predictions[order]

    # reorder rows
    predictions = predictions.sort_values(["matchDayIndex", "matchId"])

    # return predictions
    return predictions
