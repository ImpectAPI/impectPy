# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df
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

    # get match info
    match_data = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Match Info"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = match_data[match_data.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in fail_matches]

    # raise warnings
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(match_data[match_data.lastCalculationDate.notnull()].iterationId.unique())

    # get player match sums
    matchsums_raw = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}/player-kpis",
            method="GET"
        ).process_response(
            endpoint="PlayerMatchsums"
        ).assign(
            matchId=match
        ),
            matches),
        ignore_index=True)

    # get players
    players = pd.concat(
        map(
            lambda iteration: connection.make_api_request_limited(
                url=f"{host}/v5/customerapi/iterations/{iteration}/players",
                method="GET"
            ).process_response(
                endpoint="Players"
            ),
            iterations),
        ignore_index=True
    )[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg", "countryIds", "idMappings"]]

    # only keep first country id for each player
    country_series = players["countryIds"].explode().groupby(level=0).first()
    players["countryIds"] = players.index.to_series().map(country_series).astype("float").astype("Int64")
    players = players.rename(columns={"countryIds": "countryId"})

    # unnest mappings
    players = unnest_mappings_df(players, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get squads
    squads = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get coaches
    coaches = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/coaches",
            method="GET"
        ).process_response(
            endpoint="Coaches",
            raise_exception=False
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get kpis
    kpis = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/kpis",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get country data
    countries = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/countries",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )

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
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        coaches[["id", "name"]].rename(
            columns={"id": "coachId", "name": "coachName"}
        ),
        left_on="coachId",
        right_on="coachId",
        how="left",
        suffixes=("", "_right")
    ).merge(
        countries.rename(columns={"fifaName": "playerCountry"}),
        left_on="countryId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

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


######
#
# This function returns a pandas dataframe that contains all kpis for a
# given match aggregated per squad
#
######


def getSquadMatchsums(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadMatchsumsFromHost(matches, connection, "https://api.impect.com")

def getSquadMatchsumsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Input vor matches argument must be a list of integers")

    # get match info
    match_data = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}",
            method="GET"
        ).process_response(
            endpoint="Match Info"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = match_data[match_data.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in fail_matches]

    # raise warnings
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(match_data[match_data.lastCalculationDate.notnull()].iterationId.unique())

    # get squad match sums
    matchsums_raw = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}/squad-kpis",
            method="GET"
        ).process_response(
            endpoint="SquadMatchsums"
        ).assign(
            matchId=match
        ),
            matches),
        ignore_index=True)

    # get squads
    squads = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name", "idMappings"]]

    # get coaches
    coaches = pd.concat(
        map(lambda iteration: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/coaches",
            method="GET"
        ).process_response(
            endpoint="Coaches",
            raise_exception=False
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # unnest mappings
    squads = unnest_mappings_df(squads, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

    # get kpis
    kpis = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/kpis",
        method="GET"
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatchesFromHost(
            iteration=iteration,
            connection=connection,
            host=host
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # create empty df to store matchsums
    matchsums = pd.DataFrame()

    # manipulate matchsums

    # iterate over matches
    for i in range(len(matchsums_raw)):

        # iterate over sides
        for side in ["squadHomeKpis", "squadAwayKpis"]:
            # get data for index
            temp = matchsums_raw[side].loc[i]

            # convert to pandas df
            temp = pd.DataFrame(temp).assign(
                matchId=matchsums_raw.matchId.loc[i],
                squadId=matchsums_raw[side.replace("Kpis", "Id")].loc[i]
            )

            # merge with kpis to ensure all kpis are present
            temp = temp.merge(
                kpis,
                left_on="kpiId",
                right_on="id",
                how="outer",
                suffixes=("", "right")
            )

            # pivot data
            temp = pd.pivot_table(
                temp,
                values="value",
                index=["matchId", "squadId"],
                columns="name",
                aggfunc="sum",
                fill_value=0,
                dropna=False
            ).reset_index()

            # append to matchsums
            matchsums = pd.concat([matchsums, temp])

    # merge with other data
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
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_home")
    ).merge(
        coaches[["id", "name"]].rename(
            columns={"id": "coachId", "name": "coachName"}
        ),
        left_on="coachId",
        right_on="coachId",
        how="left",
        suffixes=("", "_right")
    )

    # rename some columns
    matchsums = matchsums.rename(columns={
        "scheduledDate": "dateTime"
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
        "wyscoutId",
        "heimSpielId",
        "skillCornerId",
        "squadName",
        "coachId",
        "coachName"
    ]

    # add kpiNames to order
    order += kpis['name'].to_list()

    # filter for non-NA columns only
    matchsums = matchsums[
        (matchsums.matchId.notnull()) &
        (matchsums.squadId.notnull())
    ]

    # reset index
    matchsums = matchsums.reset_index()

    # select & order columns
    matchsums = matchsums[order]

    # fix some column types
    matchsums["matchId"] = matchsums["matchId"].astype("Int64")
    matchsums["competitionId"] = matchsums["competitionId"].astype("Int64")
    matchsums["iterationId"] = matchsums["iterationId"].astype("Int64")
    matchsums["matchDayIndex"] = matchsums["matchDayIndex"].astype("Int64")
    matchsums["squadId"] = matchsums["squadId"].astype("Int64")
    matchsums["wyscoutId"] = matchsums["wyscoutId"].astype("Int64")
    matchsums["heimSpielId"] = matchsums["heimSpielId"].astype("Int64")
    matchsums["skillCornerId"] = matchsums["skillCornerId"].astype("Int64")

    # return data
    return matchsums