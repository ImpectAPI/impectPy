# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df, ForbiddenError, safe_execute, resolve_matches
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains all scores for a
# given match aggregated per squad
#
######


def getSquadMatchScores(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of per-squad scores for the given list of match IDs."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadMatchScoresFromHost(matches, connection, "https://api.impect.com")

def getSquadMatchScoresFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch per-squad scores for the given matches from the given host and return them as a DataFrame.

    Pivots raw score data per squad, merges squad IDs, coach names, and competition metadata,
    and returns one row per squad per match.
    """
    resolved = resolve_matches(matches, connection, host)
    match_data = resolved.match_data
    matches = resolved.matches
    iterations = resolved.iterations
    forbidden_matches = []

    # get squad match scores
    def fetch_squad_match_scores(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Squad Match Sums")

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
        endpoint="SquadScores"
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
                suffixes=("", "_scores")
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
        suffixes=("", "_matchplan")
    ).merge(
        pd.concat([
            match_data[["id","squadHomeId", "squadHomeCoachId"]].rename(columns={"squadHomeId": "squadId", "squadHomeCoachId": "coachId"}),
            match_data[["id","squadAwayId", "squadAwayCoachId"]].rename(columns={"squadAwayId": "squadId", "squadAwayCoachId": "coachId"})
        ], ignore_index=True),
        left_on=["matchId", "squadId"],
        right_on=["id", "squadId"],
        how="left",
        suffixes=("", "_matchData")
    ).merge(
        iterations[["id", "competitionId", "competitionName", "competitionType", "season"]],
        left_on="iterationId",
        right_on="id",
        how="left",
        suffixes=("", "_iterations")
    ).merge(
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId", "name"]].rename(
            columns={"id": "squadId", "name": "squadName"}
        ),
        left_on="squadId",
        right_on="squadId",
        how="left",
        suffixes=("", "_squads")
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
        "optaId",
        "statsPerformId",
        "transfermarktId",
        "soccerdonnaId",
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
    squad_scores["optaId"] = squad_scores["optaId"].astype("string")
    squad_scores["statsPerformId"] = squad_scores["statsPerformId"].astype("string")
    squad_scores["transfermarktId"] = squad_scores["transfermarktId"].astype("string")
    squad_scores["soccerdonnaId"] = squad_scores["soccerdonnaId"].astype("string")

    # return data
    return squad_scores