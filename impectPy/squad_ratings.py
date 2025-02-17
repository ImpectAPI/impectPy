# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI
from .iterations import getIterations
import json

######
#
# This function returns a pandas dataframe that contains all squad ratings for a given iteration
#
######


# define function
def getSquadRatings(iteration: int, token: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Argument 'iteration' must be an integer.")

    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)

    # raise exception if provided iteration id doesn't exist
    if iteration not in list(iterations.id):
        raise Exception("The supplied iteration id does not exist. Execution stopped.")

    # get squads
    squads = rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="Squads"
        )

    # get squad ratings
    ratings_raw = rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads/ratings",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="Squad Ratings"
        )

    # extract JSON from the column
    nested_data = ratings_raw["squadRatingsEntries"][0]

    # flatten ratings df
    ratings = []
    for entry in nested_data:
        date = entry["date"]
        for squad in entry["squadRatings"]:
            ratings.append({
                "date": date,
                "squadId": squad["squadId"],
                "value": squad["value"]
            })

    # convert to df
    ratings = pd.DataFrame(ratings)

    # add iteration id
    ratings["iterationId"] = iteration

    # merge with competition info
    ratings = ratings.merge(
        iterations,
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge events with squads
    ratings = ratings.merge(
        squads[["id", "name"]].rename(columns={"id": "squadId", "name": "squadName"}),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    )

    # define desired column order
    order = [
        "iterationId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "competitionId",
        "competitionName",
        "competitionType",
        "season",
        "competitionCountryId",
        "competitionGender",
        "date",
        "squadId",
        "squadName",
        "value"
    ]

    # reorder data
    ratings = ratings[order]

    # reorder rows
    ratings = ratings.sort_values(["date", "squadId"])

    # return events
    return ratings