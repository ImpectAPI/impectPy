# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df, ForbiddenError, safe_execute, resolve_matches
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost


######
#
# This function returns a pandas dataframe that contains all kpis for a
# given match aggregated per squad
#
######


def getSquadMatchsums(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of per-squad KPI sums for the given list of match IDs."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadMatchsumsFromHost(matches, connection, "https://api.impect.com")

def getSquadMatchsumsFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch per-squad KPI sums for the given matches from the given host and return them as a DataFrame.

    Pivots raw KPI data per squad, merges squad IDs, coach names, and competition metadata,
    and returns one row per squad per match.
    """
    resolved = resolve_matches(matches, connection, host)
    match_data = resolved.match_data
    matches = resolved.matches
    iterations = resolved.iterations
    forbidden_matches = []

    # get squad match sums
    def fetch_squad_match_sums(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Squad Match Sums")

    # create list to store dfs
    matchsums_list = []
    for match in matches:
        matchsums = safe_execute(
            fetch_squad_match_sums,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}/squad-kpis",
            identifier=f"{match}",
            forbidden_list=forbidden_matches
        ).assign(matchId=match)
        matchsums_list.append(matchsums)
    matchsums_raw = pd.concat(matchsums_list).reset_index(drop=True)

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
        suffixes=("", "_home")
    )

    if not coaches_blacklisted:

        # create coaches map
        coaches_map = coaches.set_index("id")["name"].to_dict()

        # convert coachId to integer if it is None
        matchsums["coachId"] = matchsums["coachId"].astype("Int64")
        matchsums["coachName"] = matchsums.coachId.map(coaches_map)

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
        "optaId",
        "statsPerformId",
        "transfermarktId",
        "soccerdonnaId",
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

    # check if coaches are blacklisted
    if coaches_blacklisted:
        order = [col for col in order if col not in ["coachId", "coachName"]]

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
    matchsums["optaId"] = matchsums["optaId"].astype("string")
    matchsums["statsPerformId"] = matchsums["statsPerformId"].astype("string")
    matchsums["transfermarktId"] = matchsums["transfermarktId"].astype("string")
    matchsums["soccerdonnaId"] = matchsums["soccerdonnaId"].astype("string")

    # return data
    return matchsums