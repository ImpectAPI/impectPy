# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all kpis for a
# given iteration aggregated per player and position
#
######


def getPlayerIterationAverages(
        iteration: int, token: str, session: requests.Session = requests.Session()
) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getPlayerIterationAveragesFromHost(iteration, connection, "https://api.impect.com")

def getPlayerIterationAveragesFromHost(
        iteration: int, connection: RateLimitedAPI, host: str
) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Input vor iteration argument must be an integer")

    # get squads
    squads = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
        method="GET"
    ).process_response(
        endpoint="Squads"
    )

    # get squadIds
    squad_ids = squads[squads.access].id.to_list()

    # get player iteration averages per squad
    averages_raw = pd.concat(
        map(lambda squadId: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/"
                f"squads/{squadId}/player-kpis",
            method="GET"
        ).process_response(
            endpoint="PlayerAverages"
        ).assign(
            iterationId=iteration,
            squadId=squadId
        ),
            squad_ids),
        ignore_index=True)

    # get players
    players = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/players",
        method="GET"
    ).process_response(
        endpoint="Players"
    )[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg", "idMappings"]]

    # unnest mappings
    players = unnest_mappings_df(players, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get kpis
    kpis = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/kpis",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # unnest scorings
    averages = averages_raw.explode("kpis").reset_index(drop=True)

    # unnest dictionary in kpis column
    averages = pd.concat(
        [averages.drop(["kpis"], axis=1), pd.json_normalize(averages["kpis"])],
        axis=1
    )

    # merge with kpis to ensure all kpis are present
    averages = averages.merge(
        kpis,
        left_on="kpiId",
        right_on="id",
        how="outer",
        suffixes=("", "_right")
    )

    # get matchShares
    match_shares = averages[
        ["iterationId", "squadId", "playerId", "position", "playDuration", "matchShare"]].drop_duplicates()

    # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
    if len(averages["name"][averages["name"].isnull()]) > 0:
        averages["name"] = averages["name"].fillna("-1")

    # pivot kpi values
    averages = pd.pivot_table(
        averages,
        values="value",
        index=["iterationId", "squadId", "playerId", "position"],
        columns="name",
        aggfunc="sum",
        fill_value=0,
        dropna=False
    ).reset_index()

    # drop "-1" column
    if "-1" in averages.columns:
        averages.drop(["-1"], inplace=True, axis=1)

    # merge with playDuration and matchShare
    averages = averages.merge(
        match_shares,
        left_on=["iterationId", "squadId", "playerId", "position"],
        right_on=["iterationId", "squadId", "playerId", "position"],
        how="inner",
        suffixes=("", "_right")
    )
    # merge with other data
    averages = averages.merge(
        iterations[["id", "competitionName", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        squads[["id", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[[
            "id", "wyscoutId", "heimSpielId", "skillCornerId", "commonname",
            "firstname", "lastname", "birthdate", "birthplace", "leg"
        ]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="playerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )
    
    # remove NA rows
    averages = averages[averages.iterationId.notnull()]

    # fix column types
    averages["iterationId"] = averages["iterationId"].astype("Int64")
    averages["squadId"] = averages["squadId"].astype("Int64")
    averages["playerId"] = averages["playerId"].astype("Int64")
    averages["wyscoutId"] = averages["wyscoutId"].astype("Int64")
    averages["heimSpielId"] = averages["heimSpielId"].astype("Int64")
    averages["skillCornerId"] = averages["skillCornerId"].astype("Int64")

    # define column order
    order = [
        "iterationId",
        "competitionName",
        "season",
        "squadId",
        "squadName",
        "playerId",
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "playerName",
        "firstname",
        "lastname",
        "birthdate",
        "birthplace",
        "leg",
        "position",
        "matchShare",
        "playDuration"
    ]

    # add kpiNames to order
    order = order + kpis.name.to_list()

    # select columns
    averages = averages[order]

    # return result
    return averages


######
#
# This function returns a pandas dataframe that contains all kpis for a
# given iteration aggregated per squad
#
######
def getSquadIterationAverages(
        iteration: int, token: str, session: requests.Session = requests.Session()
    ) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadIterationAveragesFromHost(iteration, connection, "https://api.impect.com")

def getSquadIterationAveragesFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Input vor iteration argument must be an integer")

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
    averages_raw = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squad-kpis",
            method="GET"
        ).process_response(
        endpoint="SquadAverages"
    ).assign(iterationId=iteration)

    # get kpis
    kpis = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/kpis",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get matches played
    matches = averages_raw[["squadId", "matches"]].drop_duplicates()

    # unnest scorings
    averages = averages_raw.explode("kpis").reset_index(drop=True)

    # unnest dictionary in kpis column
    averages = pd.concat(
        [averages.drop(["kpis"], axis=1), pd.json_normalize(averages["kpis"])],
        axis=1
    )

    # merge with kpis to ensure all kpis are present
    averages = averages.merge(
        kpis,
        left_on="kpiId",
        right_on="id",
        how="outer",
        suffixes=("", "_right")
    )

    # pivot kpi values
    averages = pd.pivot_table(
        averages,
        values="value",
        index=["iterationId", "squadId"],
        columns="name",
        aggfunc="sum",
        fill_value=0,
        dropna=False
    ).reset_index()

    # inner join with matches played
    averages = pd.merge(
        averages,
        matches,
        left_on="squadId",
        right_on="squadId",
        how="inner",
        suffixes=("", "_right")
    )

    # merge with other data
    averages = averages.merge(
        iterations[["id", "competitionName", "season"]],
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
    averages = averages[averages.iterationId.notnull()]

    # fix column types
    averages["squadId"] = averages["squadId"].astype("Int64")
    averages["matches"] = averages["matches"].astype("Int64")
    averages["iterationId"] = averages["iterationId"].astype("Int64")
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

    # add kpiNames to order
    order = order + kpis.name.to_list()

    # select columns
    averages = averages[order]

    # return result
    return averages
