# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI
from .matches import getMatches
from .iterations import getIterations


######
#
# This function returns a pandas dataframe that contains all kpis for a
# given match aggregated per player and position
#
######


def getPlayerMatchsums(matches: list, token: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not type(matches) == list:
        raise Exception("Argument 'matches' must be a list of integers.")

    # get match info
    iterations = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="Iterations"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = iterations[iterations.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in fail_matches]

    # raise warnings
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(iterations[iterations.lastCalculationDate.notnull()].iterationId.unique())

    # get player match sums
    matchsums_raw = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}/player-kpis",
            method="GET",
            headers=my_header
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
            lambda iteration: rate_limited_api.make_api_request_limited(
                url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/players",
                method="GET",
                headers=my_header
            ).process_response(
                endpoint="Players"
            ),
            iterations),
        ignore_index=True)[["id", "firstname", "lastname", "birthdate", "birthplace", "leg"]].drop_duplicates()

    # get squads
    squads = pd.concat(
        map(lambda iteration: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get kpis
    kpis = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/kpis",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatches(
            iteration=iteration,
            token=token,
            session=rate_limited_api.session
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)

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
        matchplan,
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        iterations,
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
        players[["id", "firstname", "lastname", "birthdate", "birthplace", "leg"]],
        left_on="id",
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
        "playerId",
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
    order += kpis['name'].to_list()

    # select columns
    matchsums = matchsums[order]

    # return data
    return matchsums


######
#
# This function returns a pandas dataframe that contains all kpis for a
# given match aggregated per squad
#
######


def getSquadMatchsums(matches: list, token: str) -> pd.DataFrame:
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not type(matches) == list:
        print("Input vor matches argument must be a list of integers")

    # get match info
    iterations = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="Iterations"
        ),
            matches),
        ignore_index=True)

    # filter for matches that are unavailable
    fail_matches = iterations[iterations.lastCalculationDate.isnull()].id.drop_duplicates().to_list()

    # drop matches that are unavailable from list of matches
    matches = [match for match in matches if match not in fail_matches]

    # raise warnings
    if len(fail_matches) > 0:
        if len(matches) == 0:
            raise Exception("All supplied matches are unavailable. Execution stopped.")
        else:
            print(f"The following matches are not available yet and were ignored:\n{fail_matches}")

    # extract iterationIds
    iterations = list(iterations[iterations.lastCalculationDate.notnull()].iterationId.unique())

    # get squad match sums
    matchsums_raw = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}/squad-kpis",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="SquadMatchsums"
        ).assign(
            matchId=match
        ),
            matches),
        ignore_index=True)

    # get squads
    squads = pd.concat(
        map(lambda iteration: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/squads",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="Squads"
        ),
            iterations),
        ignore_index=True)[["id", "name"]].drop_duplicates()

    # get kpis
    kpis = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/kpis",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="KPIs"
    )[["id", "name"]]

    # get matches
    matchplan = pd.concat(
        map(lambda iteration: getMatches(
            iteration=iteration,
            token=token,
            session=rate_limited_api.session
        ),
            iterations),
        ignore_index=True)

    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)

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
        matchplan,
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    ).merge(
        iterations,
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
        suffixes=("", "_home")
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
        "squadName"
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

    # return data
    return matchsums