# load packages
import pandas as pd
import requests
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all scores for a
# given match aggregated per squad
#
######


def getSquadMatchScores(matches: list, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadMatchScoresFromHost(matches, connection, "https://api.impect.com")

def getSquadMatchScoresFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

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

    # get squad scores
    scores_raw = pd.concat(
        map(lambda match: connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/matches/{match}/squad-scores",
            method="GET"
        ).process_response(
            endpoint="SquadMatchScores"
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

    # get squad scores
    scores = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/squad-scores",
        method="GET"
    ).process_response(
        endpoint="PlayerScores"
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

    # create empty df to store squad scores
    squad_scores = pd.DataFrame()

    # manipulate squad scores

    # iterate over matches
    for i in range(len(scores_raw)):

        # iterate over sides
        for side in ["squadHomeSquadScores", "squadAwaySquadScores"]:
            # get data for index
            temp = scores_raw[side].loc[i]

            # convert to pandas df
            temp = pd.DataFrame(temp).assign(
                matchId=scores_raw.matchId.loc[i],
                squadId=scores_raw[side.replace("SquadScores", "Id")].loc[i]
            )

            # merge with squad scores to ensure all scores are present
            temp = pd.merge(
                temp,
                scores,
                left_on="squadScoreId",
                right_on="id",
                how="outer",
                suffixes=("", "_right")
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

            # append to player_scores
            squad_scores = pd.concat([squad_scores, temp])

    # merge with other data
    squad_scores = squad_scores.merge(
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
        suffixes=("", "_right")
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
    squad_scores = squad_scores.rename(columns={
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

    # add scoreNames to order
    order += scores["name"].to_list()

    # select columns
    squad_scores = squad_scores[order]

    # fix some column types
    squad_scores["matchId"] = squad_scores["matchId"].astype("Int64")
    squad_scores["competitionId"] = squad_scores["competitionId"].astype("Int64")
    squad_scores["iterationId"] = squad_scores["iterationId"].astype("Int64")
    squad_scores["matchDayIndex"] = squad_scores["matchDayIndex"].astype("Int64")
    squad_scores["squadId"] = squad_scores["squadId"].astype("Int64")
    squad_scores["wyscoutId"] = squad_scores["wyscoutId"].astype("Int64")
    squad_scores["heimSpielId"] = squad_scores["heimSpielId"].astype("Int64")
    squad_scores["skillCornerId"] = squad_scores["skillCornerId"].astype("Int64")

    # return data
    return squad_scores

######
#
# This function returns a pandas dataframe that contains all scores for a
# given iteration aggregated per squad
#
######


def getSquadIterationScores(iteration: int, token: str, session: requests.Session = requests.Session()) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadIterationScoresFromHost(iteration, connection, "https://api.impect.com")

def getSquadIterationScoresFromHost(iteration: int, connection: RateLimitedAPI, host: str) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(iteration, int):
        raise Exception("Input for iteration argument must be an integer")

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
    scores_raw = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/iterations/{iteration}/squad-scores",
        method="GET"
    ).process_response(
        endpoint="SquadIterationScores"
    ).assign(iterationId=iteration)

    # get scores
    scores_definitions = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/squad-scores",
        method="GET"
    ).process_response(
        endpoint="scoreDefinitions"
    )[["id", "name"]]

    # get iterations
    iterations = getIterationsFromHost(connection=connection, host=host)

    # get matches played
    matches = scores_raw[["squadId", "matches"]].drop_duplicates()

    # unnest scores
    scores = scores_raw.explode("squadScores").reset_index(drop=True)

    # unnest dictionary in kpis column
    scores = pd.concat(
        [scores.drop(["squadScores"], axis=1), pd.json_normalize(scores["squadScores"])],
        axis=1
    )

    # merge with kpis to ensure all kpis are present
    scores = scores.merge(
        scores_definitions,
        left_on="squadScoreId",
        right_on="id",
        how="outer",
        suffixes=("", "_right")
    )

    # pivot kpi values
    scores = pd.pivot_table(
        scores,
        values="value",
        index=["iterationId", "squadId"],
        columns="name",
        aggfunc="sum",
        fill_value=0,
        dropna=False
    ).reset_index()

    # inner join with matches played
    scores = pd.merge(
        scores,
        matches,
        left_on="squadId",
        right_on="squadId",
        how="inner",
        suffixes=("", "_right")
    )

    # merge with other data
    scores = scores.merge(
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
        suffixes=("", "_right")
    )

    # remove NA rows
    averages = scores[scores.iterationId.notnull()]

    # fix column types
    averages["matches"] = averages["matches"].astype("Int64")
    averages["iterationId"] = averages["iterationId"].astype("Int64")
    averages["squadId"] = averages["squadId"].astype("Int64")
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

    # add scoreNames to order
    order = order + scores_definitions.name.to_list()

    # select columns
    averages = averages[order]

    # return result
    return averages