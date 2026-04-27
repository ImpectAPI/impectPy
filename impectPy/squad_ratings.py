# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all squad ratings for a given iteration
#
######


# define function
def getSquadRatings(iteration: int, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of squad ratings for all dates in the given iteration."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadRatingsFromHost(iteration, connection, "https://api.impect.com")

def getSquadRatingsFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch squad ratings for the given iteration from the given host and return them as a DataFrame.

    Flattens the nested ratings structure, merges competition and squad metadata, and sorts by
    date and squad ID. Returns an empty DataFrame when no ratings are available.
    """
    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Argument 'iteration' must be an integer.")

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

    # get squad ratings
    ratings_raw = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads/ratings",
            method="GET"
        ).process_response(
            endpoint="Squad Ratings"
        )

    # check if response is empty
    if ratings_raw.empty or not ratings_raw["squadRatingsEntries"].iloc[0]:
        return pd.DataFrame()

    # extract JSON from the column
    nested_data = ratings_raw["squadRatingsEntries"].iloc[0]

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
        iterations[["id", "competitionId", "competitionName", "competitionType", "season", "competitionGender"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
    )

    # merge events with squads
    ratings = ratings.merge(
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    )

    # fix some column types
    ratings["iterationId"] = ratings["iterationId"].astype("Int64")
    ratings["competitionId"] = ratings["competitionId"].astype("Int64")
    ratings["squadId"] = ratings["squadId"].astype("Int64")
    ratings["wyscoutId"] = ratings["wyscoutId"].astype("Int64")
    ratings["heimSpielId"] = ratings["heimSpielId"].astype("Int64")
    ratings["skillCornerId"] = ratings["skillCornerId"].astype("Int64")
    ratings["optaId"] = ratings["optaId"].astype("string")
    ratings["statsPerformId"] = ratings["statsPerformId"].astype("string")
    ratings["transfermarktId"] = ratings["transfermarktId"].astype("string")
    ratings["soccerdonnaId"] = ratings["soccerdonnaId"].astype("string")

    # define desired column order
    order = [
        "iterationId",
        "competitionId",
        "competitionName",
        "competitionType",
        "season",
        "competitionGender",
        "date",
        "squadId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "optaId",
        "statsPerformId",
        "transfermarktId",
        "soccerdonnaId",
        "squadName",
        "value"
    ]

    # reorder data
    ratings = ratings[order]

    # reorder rows
    ratings = ratings.sort_values(["date", "squadId"])

    # return events
    return ratings