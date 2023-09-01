# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI
from .iterations import getIterations


######
#
# This function returns a pandas dataframe that contains all kpis for a
# given iteration aggregated per player and position
#
######


def getPlayerIterationAverages(iteration: int, token: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not type(iteration) == int:
        print("Input vor iteration argument must be an integer")

    # get squads
    squads = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="Squads"
    )

    # get squadIds
    squad_ids = squads[squads.access].id.to_list()

    # get player iteration averages per squad
    averages_raw = pd.concat(
        map(lambda squadId: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/"
                f"squads/{squadId}/player-kpis",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="PlayerAverages"
        ).assign(
            iterationId=iteration,
            squadId=squadId
        ),
            squad_ids),
        ignore_index=True)

    # get players
    players = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/players",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="Players"
    )

    # get kpis
    kpis = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/kpis",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)

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
        players[["id", "commonname"]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="playerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # fix column types
    averages["squadId"] = averages["squadId"].astype(int)
    averages["playerId"] = averages["playerId"].astype(int)
    averages["iterationId"] = averages["iterationId"].astype(int)

    # define column order
    order = [
        "iterationId",
        "competitionName",
        "season",
        "squadId",
        "squadName",
        "playerId",
        "playerName",
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


def getSquadIterationAverages(iteration: int, token: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not type(iteration) == int:
        print("Input vor iteration argument must be an integer")

    # get squads
    squads = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="Squads"
    )

    # get squad iteration averages
    averages_raw = rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squad-kpis",
            method="GET",
            headers=my_header
        ).process_response(
        endpoint="SquadAverages"
    ).assign(iterationId=iteration)

    # get kpis
    kpis = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/kpis",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)

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
        squads[["id", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_right")
    )

    # fix column types
    averages["squadId"] = averages["squadId"].astype(int)
    averages["matches"] = averages["matches"].astype(int)
    averages["iterationId"] = averages["iterationId"].astype(int)

    # define column order
    order = [
        "iterationId",
        "competitionName",
        "season",
        "squadId",
        "squadName",
        "matches"
    ]

    # add kpiNames to order
    order = order + kpis.name.to_list()

    # select columns
    averages = averages[order]

    # return result
    return averages
