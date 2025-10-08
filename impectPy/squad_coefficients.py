# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all squad ratings for a given iteration
#
######


# define function
def getSquadCoefficients(iteration: int, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadCoefficientsFromHost(iteration, connection, "https://api.impect.com")

def getSquadCoefficientsFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Argument 'iteration' must be an integer.")

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # raise exception if provided iteration id doesn't exist
    if iteration not in list(iterations.id):
        raise Exception("The supplied iteration id does not exist. Execution stopped.")

    # get squads
    squads = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        )[["id", "name", "idMappings"]]

    # unnest mappings
    squads = unnest_mappings_df(squads, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get squad coefficients
    coefficients_raw = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/predictions/model-coefficients",
        method="GET"
    ).process_response(
        endpoint="Squad Coefficients"
    )

    # extract JSON from the column
    nested_data = coefficients_raw["entries"][0]

    # flatten coefficients df
    coefficients = []
    for entry in nested_data:
        date = entry["date"]
        for squad in entry["squads"]:
            coefficients.append({
                "iterationId": iteration,
                "date": date,
                "interceptCoefficient": entry["competition"]["intercept"],
                "homeCoefficient": entry["competition"]["home"],
                "competitionCoefficient": entry["competition"]["comp"],
                "squadId": squad["id"],
                "attackCoefficient": squad["att"],
                "defenseCoefficient": squad["def"]
            })

    # convert to df
    coefficients = pd.DataFrame(coefficients)

    # merge with competition info
    coefficients = coefficients.merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season", "competitionGender"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # merge events with squads
    coefficients = coefficients.merge(
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    )

    # fix some column types
    coefficients["iterationId"] = coefficients["iterationId"].astype("Int64")
    coefficients["competitionId"] = coefficients["competitionId"].astype("Int64")
    coefficients["squadId"] = coefficients["squadId"].astype("Int64")
    coefficients["wyscoutId"] = coefficients["wyscoutId"].astype("Int64")
    coefficients["heimSpielId"] = coefficients["heimSpielId"].astype("Int64")
    coefficients["skillCornerId"] = coefficients["skillCornerId"].astype("Int64")

    # define desired column order
    order = [
        "iterationId",
        "competitionId",
        "competitionName",
        "competitionType",
        "season",
        "competitionGender",
        "interceptCoefficient",
        "homeCoefficient",
        "competitionCoefficient",
        "date",
        "squadId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "squadName",
        "attackCoefficient",
        "defenseCoefficient",
    ]

    # reorder data
    coefficients = coefficients[order]

    # reorder rows
    coefficients = coefficients.sort_values(["date", "squadId"])

    # return events
    return coefficients