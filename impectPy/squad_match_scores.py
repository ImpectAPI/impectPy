# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df, ForbiddenError, safe_execute
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

    # get squad match scores
    def fetch_squad_match_scores(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Player Match Sums")

    # create list to store dfs
    scores_list = []
    for match in matches:
        scores = safe_execute(
            fetch_squad_match_scores,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}/squad-scores",
            identifier=f"{match}",
            forbidden_list=forbidden_matches
        ).assign(matchId=match)
        scores_list.append(scores)
    scores_raw = pd.concat(scores_list).reset_index(drop=True)

    # get squads
    squads_list = []
    for iteration in iterations:
        squads = connection.make_api_request_limited(
            url=f"{host}/v5/customerapi/iterations/{iteration}/squads",
            method="GET"
        ).process_response(
            endpoint="Squads"
        )[["id", "name", "idMappings"]]
        squads_list.append(squads)
    squads = pd.concat(squads_list).drop_duplicates("id").reset_index(drop=True)

    # unnest mappings
    squads = unnest_mappings_df(squads, "idMappings").drop(["idMappings"], axis=1).drop_duplicates()

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

    # get squad scores
    scores = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/squad-scores",
        method="GET"
    ).process_response(
        endpoint="PlayerScores"
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
    )

    if not coaches_blacklisted:

        # create coaches map
        coaches_map = coaches.set_index("id")["name"].to_dict()

        # convert coachId to integer if it is None
        squad_scores["coachId"] = squad_scores["coachId"].astype("Int64")
        squad_scores["coachName"] = squad_scores.coachId.map(coaches_map)

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

    # check if coaches are blacklisted
    if coaches_blacklisted:
        order = [col for col in order if col not in ["coachId", "coachName"]]

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