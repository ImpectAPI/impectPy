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

    # get players
    players = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/players",
        method="GET"
    ).process_response(
        endpoint="Players"
    )[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg", "countryIds", "idMappings"]]

    # only keep first country id for each player
    country_series = players["countryIds"].explode().groupby(level=0).first()
    players["countryIds"] = players.index.to_series().map(country_series).astype("float").astype("Int64")
    players = players.rename(columns={"countryIds": "countryId"})

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

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )

    # create empty df to store averages
    averages_list = []

    # iterate over squads
    for squad_id in squad_ids:

        # get player iteration averages per squad
        averages_raw = connection.make_api_request_limited(
                url=f"{host}/v5/customerapi/iterations/{iteration}/"
                    f"squads/{squad_id}/player-kpis",
                method="GET"
            ).process_response(
                endpoint="PlayerAverages",
                raise_exception=False
            ).assign(
                iterationId=iteration,
                squadId=squad_id
            )

        # skip if empty repsonse
        if len(averages_raw) == 0:
            continue

        # unnest scorings
        averages_raw = averages_raw.explode("kpis").reset_index(drop=True)

        # unnest dictionary in kpis column
        averages_raw = pd.concat(
            [averages_raw.drop(["kpis"], axis=1), pd.json_normalize(averages_raw["kpis"])],
            axis=1
        )

        # merge with kpis to ensure all kpis are present
        averages_raw = averages_raw.merge(
            kpis,
            left_on="kpiId",
            right_on="id",
            how="outer",
            suffixes=("", "_right")
        )

        # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
        if len(averages_raw["name"][averages_raw["name"].isnull()]) > 0:
            averages_raw["name"] = averages_raw["name"].fillna("-1")

        # get KPIs without a scoring
        mask = (
                averages_raw.iterationId.isnull()
                & averages_raw.squadId.isnull()
                & averages_raw.playerId.isnull()
                & averages_raw.position.isnull()
        )

        # fill join cols with placeholder
        filled = (
            averages_raw.loc[mask]
            .infer_objects(copy=False)
            .fillna(-1)
        )

        averages_raw.loc[mask] = filled

        # get matchShares
        match_shares_raw = averages_raw[
            ["iterationId", "squadId", "playerId", "position", "playDuration", "matchShare"]].drop_duplicates()

        # pivot kpi values
        averages_raw = pd.pivot_table(
            averages_raw,
            values="value",
            index=["iterationId", "squadId", "playerId", "position"],
            columns="name",
            aggfunc="sum",
            fill_value=0,
            dropna=False,
            observed=True,
        ).reset_index()

        # drop "-1" column
        if "-1" in averages_raw.columns:
            averages_raw.drop(["-1"], inplace=True, axis=1)

        # drop -1 rows
        averages_raw = averages_raw[
            ~(averages_raw.iterationId == -1)
            & ~(averages_raw.squadId == -1)
            & ~(averages_raw.playerId == -1)
            & ~(averages_raw.position == -1)
        ]

        # merge with playDuration and matchShare
        averages_raw = averages_raw.merge(
            match_shares_raw,
            left_on=["iterationId", "squadId", "playerId", "position"],
            right_on=["iterationId", "squadId", "playerId", "position"],
            how="inner",
            suffixes=("", "_right")
        )

        averages_list.append(averages_raw)

    # compile into df
    averages = pd.concat(averages_list)

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
            "firstname", "lastname", "birthdate", "birthplace", "countryId", "leg"
        ]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="playerId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        countries.rename(columns={"fifaName": "playerCountry"}),
        left_on="countryId",
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
        "playerCountry",
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