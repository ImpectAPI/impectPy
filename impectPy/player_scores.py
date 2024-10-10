# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI
from .matches import getMatches
from .iterations import getIterations


######
#
# This function returns a pandas dataframe that contains all scores for a
# given match and a given set of positions aggregated per player
#
######


def getPlayerMatchScores(matches: list, positions: list, token: str) -> pd.DataFrame:
    
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()

    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # check input for matches argument
    if not isinstance(matches, list):
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
    
    # compile list of positions
    position_string = ",".join(positions)

    # get player scores
    scores_raw = pd.concat(
        map(lambda match: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/matches/{match}/positions/{position_string}/player-scores",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="PlayerMatchScores"
        ).assign(
            matchId=match,
            positions=position_string
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
        ignore_index=True)[
        ["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg"]
    ].drop_duplicates()

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

    # get player scores
    scores = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/player-scores",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="PlayerScores"
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

    # create empty df to store player scores
    player_scores = pd.DataFrame()

    # manipulate player_scores

    # iterate over matches
    for i in range(len(scores_raw)):

        # iterate over sides
        for side in ["squadHomePlayers", "squadAwayPlayers"]:

            # get data for index
            temp = scores_raw[side].loc[i]

            # convert to pandas df
            temp = pd.DataFrame(temp).assign(
                matchId=scores_raw.matchId.loc[i],
                squadId=scores_raw[side.replace("Players", "Id")].loc[i],
                positions=scores_raw.positions.loc[i]
            )

            # extract matchshares
            matchshares = temp[["matchId", "squadId", "id", "matchShare", "playDuration"]].drop_duplicates().assign(
                positions=position_string
            )

            # explode kpis column
            temp = temp.explode("playerScores")

            # unnest dictionary in kpis column
            temp = pd.concat(
                [temp.drop(["playerScores"], axis=1), temp["playerScores"].apply(pd.Series)],
                axis=1
            )

            # merge with player scores to ensure all scores are present
            temp = pd.merge(
                temp,
                scores,
                left_on="playerScoreId",
                right_on="id",
                how="outer",
                suffixes=("", "_right")
            )

            # pivot data
            temp = pd.pivot_table(
                temp,
                values="value",
                index=["matchId", "squadId", "positions", "id"],
                columns="name",
                aggfunc="sum",
                fill_value=0,
                dropna=False
            ).reset_index()

            # inner join with matchshares
            temp = pd.merge(
                temp,
                matchshares,
                left_on=["matchId", "squadId", "id", "positions"],
                right_on=["matchId", "squadId", "id", "positions"],
                how="inner",
                suffixes=("", "_right")
            )

            # append to player_scores
            player_scores = pd.concat([player_scores, temp])

    # merge with other data
    player_scores = player_scores.merge(
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
        players[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg"]].rename(
            columns={"commonname": "playerName"}
        ),
        left_on="id",
        right_on="id",
        how="left",
        suffixes=("", "_right")
    )

    # rename some columns
    player_scores = player_scores.rename(columns={
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
        "playerName",
        "firstname",
        "lastname",
        "birthdate",
        "birthplace",
        "leg",
        "positions",
        "matchShare",
        "playDuration",
    ]

    # add kpiNames to order
    order += scores["name"].to_list()

    # select columns
    player_scores = player_scores[order]
    
    # return data
    return player_scores


######
#
# This function returns a pandas dataframe that contains all scores for a
# given iteration and a given set of positions aggregated per player
#
######


def getPlayerIterationScores(iteration: int, positions: list, token: str) -> pd.DataFrame:
    
    # create an instance of RateLimitedAPI
    rate_limited_api = RateLimitedAPI()
    
    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}
    
    # check input for matches argument
    if not isinstance(iteration, int):
        print("Input for iteration argument must be an integer")
    
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
    
    # compile position string
    position_string = ",".join(positions)
    
    # get player iteration averages per squad
    scores_raw = pd.concat(
        map(lambda squadId: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/"
                f"squads/{squadId}/positions/{position_string}/player-scores",
            method="GET",
            headers=my_header
        ).process_response(
            endpoint="PlayerIterationScores"
        ).assign(
            iterationId=iteration,
            squadId=squadId,
            positions=position_string
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
    
    # get scores
    scores = rate_limited_api.make_api_request_limited(
        url=f"https://api.impect.com/v5/customerapi/player-scores",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="playerScores"
    )[["id", "name"]]
    
    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)
    
    # unnest scorings
    averages = scores_raw.explode("playerScores").reset_index(drop=True)
    
    # unnest dictionary in kpis column
    averages = pd.concat(
        [averages.drop(["playerScores"], axis=1), pd.json_normalize(averages["playerScores"])],
        axis=1
    )
    
    # merge with player scores to ensure all kpis are present
    averages = averages.merge(
        scores,
        left_on="playerScoreId",
        right_on="id",
        how="outer",
        suffixes=("", "_right")
    )
    
    # get matchShares
    match_shares = averages[
        ["iterationId", "squadId", "playerId", "positions", "playDuration", "matchShare"]].drop_duplicates()
    
    # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
    if len(averages["name"][averages["name"].isnull()]) > 0:
        averages["name"] = averages["name"].fillna("-1")
    
    # pivot kpi values
    averages = pd.pivot_table(
        averages,
        values="value",
        index=["iterationId", "squadId", "playerId", "positions"],
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
        left_on=["iterationId", "squadId", "playerId", "positions"],
        right_on=["iterationId", "squadId", "playerId", "positions"],
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
        players[["id", "commonname", "firstname", "lastname", "birthdate", "birthplace", "leg"]].rename(
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
        "firstname",
        "lastname",
        "birthdate",
        "birthplace",
        "leg",
        "positions",
        "matchShare",
        "playDuration"
    ]
    
    # add kpiNames to order
    order = order + scores.name.to_list()
    
    # select columns
    averages = averages[order]
    
    # return result
    return averages