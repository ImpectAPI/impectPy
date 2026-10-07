# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains the style of play
# values for a given iteration per squad
#
######


def getSquadIterationStyleOfPlay(iteration: int, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of per-squad iteration-level style of play values for the given iteration."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadIterationStyleOfPlayFromHost(iteration, connection, "https://api.impect.com")

def getSquadIterationStyleOfPlayFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch per-squad iteration-level style of play values for the given iteration from the given host.

    Pivots raw squad style of play data, merges squad IDs and competition metadata, and returns
    one row per squad with match count and one column per style of play.
    """
    # check input for iteration argument
    if not isinstance(iteration, int):
        raise Exception("Argument 'iteration' must be an integer.")

    # get squads
    squads = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
        method="GET"
    ).process_response(
        endpoint="Squads"
    )[["id", "name", "idMappings"]]

    # unnest mappings
    squads = unnest_mappings_df(squads, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get squad iteration style of play
    styles_raw = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squad-style-of-play",
        method="GET"
    ).process_response(
        endpoint="SquadIterationStyleOfPlay"
    ).assign(iterationId=iteration)

    # get style of play definitions
    styles_definitions = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/squad-style-of-play",
        method="GET"
    ).process_response(
        endpoint="StyleOfPlayDefinitions"
    )[["styleOfPlayName"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # unnest style of play values into long format
    styles = pd.DataFrame(
        [
            {"squadId": row.squadId, "styleOfPlayName": style["styleOfPlayName"], "value": style["value"]}
            for row in styles_raw.itertuples()
            if isinstance(row.styleOfPlay, list)
            for style in row.styleOfPlay
        ],
        columns=["squadId", "styleOfPlayName", "value"]
    )

    # pivot style of play values and ensure all styles are present
    styles = styles.pivot(
        index="squadId",
        columns="styleOfPlayName",
        values="value"
    ).reindex(
        columns=styles_definitions.styleOfPlayName.to_list()
    ).reset_index()

    # merge with squads and matches played (keeps squads without style of play values)
    averages = styles_raw[["iterationId", "squadId", "matches"]].merge(
        styles,
        left_on="squadId",
        right_on="squadId",
        how="left"
    )

    # merge with other data
    averages = averages.merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
    ).merge(
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId", "dflId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_squads")
    )

    # fix column types
    averages["matches"] = averages["matches"].astype("Int64")
    averages["iterationId"] = averages["iterationId"].astype("Int64")
    averages["squadId"] = averages["squadId"].astype("Int64")
    averages["wyscoutId"] = averages["wyscoutId"].astype("Int64")
    averages["heimSpielId"] = averages["heimSpielId"].astype("Int64")
    averages["skillCornerId"] = averages["skillCornerId"].astype("Int64")
    averages["optaId"] = averages["optaId"].astype("string")
    averages["statsPerformId"] = averages["statsPerformId"].astype("string")
    averages["transfermarktId"] = averages["transfermarktId"].astype("string")
    averages["soccerdonnaId"] = averages["soccerdonnaId"].astype("string")
    averages["dflId"] = averages["dflId"].astype("string")

    # define column order
    order = [
        "iterationId",
        "competitionName",
        "season",
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
        "matches"
    ]

    # add style of play names to order
    order = order + styles_definitions.styleOfPlayName.to_list()

    # select columns
    averages = averages[order]

    # return result
    return averages
