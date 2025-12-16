# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df, ForbiddenError, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all kpis for a
# given match aggregated per player and position
#
######


def getPlayerMatchsums(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getPlayerMatchsumsFromHost(matches, connection, "https://api.impect.com")

def getPlayerMatchsumsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # create list to store matches that are forbidden (HTTP 403)
    forbidden_matches = []

    # get match info
    def fetch_match_info(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Match Info")

    # create list to store dfs
    match_data_list = []
    for match in matches:
        match_data = safe_execute(
            fetch_match_info,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}",
            identifier=match,
            forbidden_list=forbidden_matches
        )
        match_data_list.append(match_data)
    match_data = pd.concat(match_data_list)

    # filter for matches that are unavailable
    unavailable_matches = match_data[match_data.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in unavailable_matches]

    # drop matches that are forbidden
    matches = [match for match in matches if match not in forbidden_matches]

    # configure warning format
    def no_line_formatter(message, category, filename, lineno, line):
        return f"Warning: {message}\n"
    warnings.formatwarning = no_line_formatter

    # raise exception if no matches remaining or report removed matches
    if len(matches) == 0:
        raise Exception("All supplied matches are unavailable or forbidden. Execution stopped.")
    if len(forbidden_matches) > 0:
        warnings.warn(f"The following matches are forbidden for the user: {forbidden_matches}")
    if len(unavailable_matches) > 0:
        warnings.warn(f"The following matches are not available yet and were ignored: {unavailable_matches}")

    # extract iterationIds
    iterations = list(match_data[match_data.lastCalculationDate.notnull()].iterationId.unique())

    # get player match sums
    def fetch_player_match_sums(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Player Match Sums")

    # create list to store dfs
    matchsums_list = []
    for match in matches:
        matchsums = safe_execute(
            fetch_player_match_sums,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}/player-kpis",
            identifier=f"{match}",
            forbidden_list=forbidden_matches
        ).assign(matchId=match)
        matchsums_list.append(matchsums)
    matchsums_raw = pd.concat(matchsums_list).reset_index(drop=True)

    # get players
    players_list = []
    for iteration in iterations:
        players = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/players",
            method="GET"
        ).process_response(
            endpoint="Players"
        )[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg", "countryIds", "idMappings"]]
        players_list.append(players)
    players = pd.concat(players_list).drop_duplicates("id").reset_index(drop=True)

    # only keep first country id for each player
    country_series = players["countryIds"].explode().groupby(level=0).first()
    players["countryIds"] = players.index.to_series().map(country_series).astype("float").astype("Int64")
    players = players.rename(columns={"countryIds": "countryId"})

    # unnest mappings
    players = unnest_mappings_df(players, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get squads
    squads_list = []
    for iteration in iterations:
        squads = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        )[["id", "name"]]
        squads_list.append(squads)
    squads = pd.concat(squads_list).drop_duplicates()
    squad_map = squads.set_index("id")["name"].to_dict()

    # get coaches
    coaches_blacklisted = False
    coaches_list = []
    for iteration in iterations:
        try:
            coaches = connection.make_api_request_limited(
                url=f"{host}/v5/customerapi/iterations/{iteration}/coaches",
                method="GET"
            ).process_response(
                endpoint="Coaches",
                raise_exception=False
            )[["id", "name"]]
            coaches_list.append(coaches)
        except KeyError:
            # no coaches found, create empty df
            coaches_list.append(pd.DataFrame(columns=["id", "name"]))
        except ForbiddenError:
            coaches_blacklisted = True
    coaches = pd.concat(coaches_list).drop_duplicates()

    # get kpis
    kpis = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/kpis",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get matches
    matchplan_list = []
    for iteration in iterations:
        matchplan = getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        )
        matchplan_list.append(matchplan)
    matchplan = pd.concat(matchplan_list)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    ).process_response(
        endpoint="Countries"
    )
    country_map = countries.set_index("id")["fifaName"].to_dict()

    # create empty df to store matchsums
    matchsums = pd.DataFrame()

    # manipulate matchsums

    # iterate over matches
    for i in range(len(matchsums_raw)):

        # iterate over sides
        for side in ["squadHomePlayers", "squadAwayPlayers"]:
            # get data for index
            temp = matchsums_raw[side].loc[i]

            # convert to pandas df
            temp = pd.DataFrame(temp).assign(
                matchId=matchsums_raw.matchId.loc[i],
                squadId=matchsums_raw[side.replace("Players", "Id")].loc[i]
            )

            # extract matchshares
            matchshares = temp[["matchId", "squadId", "id", "position", "matchShare", "playDuration"]].drop_duplicates()

            # explode kpis column
            temp = temp.explode("kpis")

            # unnest dictionary in kpis column
            temp = pd.concat(
                [temp.drop(["kpis"], axis=1), temp["kpis"].apply(pd.Series)],
                axis=1
            )

            # merge with kpis to ensure all kpis are present
            temp = pd.merge(
                temp,
                kpis,
                left_on="kpiId",
                right_on="id",
                how="outer",
                suffixes=("", "_right")
            )

            # pivot data
            temp = pd.pivot_table(
                temp,
                values="value",
                index=["matchId", "squadId", "id", "position"],
                columns="name",
                aggfunc="sum",
                fill_value=0,
                dropna=False
            ).reset_index()

            # inner join with matchshares
            temp = pd.merge(
                temp,
                matchshares,
                left_on=["matchId", "squadId", "id", "position"],
                right_on=["matchId", "squadId", "id", "position"],
                how="inner",
                suffixes=("", "_right")
            )

            # append to matchsums
            matchsums = pd.concat([matchsums, temp])

    # merge with other data
    matchsums["squadName"] = matchsums.squadId.map(squad_map)
    matchsums["playerCountry"] = matchsums.squadId.map(country_map)
    matchsums = matchsums.merge(
        matchplan[["id", "scheduledDate", "matchDayIndex", "matchDayName", "iterationId"]],
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        pd.concat([
            match_data[["id","squadHomeId", "squadHomeCoachId"]].rename(columns={"squadHomeId": "squadId", "squadHomeCoachId": "coachId"}),
            match_data[["id","squadAwayId", "squadAwayCoachId"]].rename(columns={"squadAwayId": "squadId", "squadAwayCoachId": "coachId"})
        ], ignore_index=True),
        left_on=["matchId", "squadId"],
        right_on=["id", "squadId"],
        how="left",
        suffixes=("", "_right")
    ).merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        players[[
            "id", "wyscoutId", "heimSpielId", "skillCornerId", "commonname",
            "firstname", "lastname", "birthdate", "birthplace", "countryId", "leg"
        ]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    if not coaches_blacklisted:

        # create coaches map
        coaches_map = coaches.set_index("id")["name"].to_dict()

        # convert coachId to integer if it is None
        matchsums["coachId"] = matchsums["coachId"].astype("Int64")
        matchsums["coachName"] = matchsums.coachId.map(coaches_map)

    # rename some columns
    matchsums = matchsums.rename(columns={
        "scheduledDate": "dateTime",
        "id": "playerId"
    })

    # define column order
    order = [
        "matchId",
        "dateTime",
        "competitionName",
        "competitionId",
        "competitionType",
        "iterationId",
        "season",
        "matchDayIndex",
        "matchDayName",
        "squadId",
        "squadName",
        "coachId",
        "coachName",
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
    order += kpis['name'].to_list()

    # check if coaches are blacklisted
    if coaches_blacklisted:
        order = [col for col in order if col not in ["coachId", "coachName"]]

    # select columns
    matchsums = matchsums[order]

    # fix some column types
    matchsums["matchId"] = matchsums["matchId"].astype("Int64")
    matchsums["squadId"] = matchsums["squadId"].astype("Int64")
    matchsums["playerId"] = matchsums["playerId"].astype("Int64")
    matchsums["wyscoutId"] = matchsums["wyscoutId"].astype("Int64")
    matchsums["heimSpielId"] = matchsums["heimSpielId"].astype("Int64")
    matchsums["skillCornerId"] = matchsums["skillCornerId"].astype("Int64")

    # return data
    return matchsums