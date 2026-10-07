# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI, ImpectSession, unnest_mappings_df, ForbiddenError, safe_execute, resolve_matches
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

######
#
# This function returns a pandas dataframe that contains the style of play
# values for a given match per squad
#
######


def getSquadMatchStyleOfPlay(matches: list, token: str, session: ImpectSession = ImpectSession()) -> pd.DataFrame:
    """Return a DataFrame of per-squad style of play values for the given list of match IDs."""
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getSquadMatchStyleOfPlayFromHost(matches, connection, "https://api.impect.com")

def getSquadMatchStyleOfPlayFromHost(matches: list, connection: RateLimitedAPI, host: str) -> pd.DataFrame:
    """Fetch per-squad style of play values for the given matches from the given host.

    Pivots raw style of play data per squad, merges squad IDs, coach names, and competition
    metadata, and returns one row per squad per match with one column per style of play.
    """
    resolved = resolve_matches(matches, connection, host)
    match_data = resolved.match_data
    matches = resolved.matches
    iterations = resolved.iterations
    forbidden_matches = []

    # get squad match style of play
    def fetch_squad_match_style_of_play(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Squad Match Style Of Play")

    # create list to store dfs
    styles_list = []
    for match in matches:
        styles = safe_execute(
            fetch_squad_match_style_of_play,
            connection,
            url=f"{host}/v5/customerapi/matches/{match}/squad-style-of-play",
            identifier=f"{match}",
            forbidden_list=forbidden_matches
        ).assign(matchId=match)
        styles_list.append(styles)
    styles_raw = pd.concat(styles_list).reset_index(drop=True)

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

    # get style of play definitions
    styles_definitions = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/squad-style-of-play",
        method="GET"
    ).process_response(
        endpoint="StyleOfPlayDefinitions"
    )[["styleOfPlayName"]]

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

    # one row per match and side, keeping sides without style of play values
    sides = []
    long_rows = []
    for row in styles_raw.to_dict("records"):
        for side in ["squadHome", "squadAway"]:
            squad_id = row.get(f"{side}Id")
            if pd.isnull(squad_id):
                continue
            sides.append({"matchId": row["matchId"], "squadId": squad_id})
            side_styles = row.get(f"{side}StyleOfPlay")
            if isinstance(side_styles, list):
                for style in side_styles:
                    long_rows.append({
                        "matchId": row["matchId"],
                        "squadId": squad_id,
                        "styleOfPlayName": style["styleOfPlayName"],
                        "value": style["value"]
                    })
    sides = pd.DataFrame(sides, columns=["matchId", "squadId"])
    styles = pd.DataFrame(long_rows, columns=["matchId", "squadId", "styleOfPlayName", "value"])

    # pivot style of play values and ensure all styles are present
    styles = styles.pivot(
        index=["matchId", "squadId"],
        columns="styleOfPlayName",
        values="value"
    ).reindex(
        columns=styles_definitions.styleOfPlayName.to_list()
    ).reset_index()

    # merge style of play values onto all match sides
    squad_styles = sides.merge(
        styles,
        on=["matchId", "squadId"],
        how="left"
    )

    # merge with other data
    squad_styles = squad_styles.merge(
        matchplan[["id", "scheduledDate", "matchDayIndex", "matchDayName", "iterationId"]],
        left_on="matchId",
        right_on="id",
        how="left",
        suffixes=("", "_matchplan")
    ).merge(
        pd.concat([
            match_data[["id", "squadHomeId", "squadHomeCoachId"]].rename(columns={"squadHomeId": "squadId", "squadHomeCoachId": "coachId"}),
            match_data[["id", "squadAwayId", "squadAwayCoachId"]].rename(columns={"squadAwayId": "squadId", "squadAwayCoachId": "coachId"})
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
        squads[["id", "wyscoutId", "heimSpielId", "skillCornerId", "optaId", "statsPerformId", "transfermarktId", "soccerdonnaId", "dflId", "name"]].rename(
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
        squad_styles["coachId"] = squad_styles["coachId"].astype("Int64")
        squad_styles["coachName"] = squad_styles.coachId.map(coaches_map)

    # rename some columns
    squad_styles = squad_styles.rename(columns={
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
        "dflId",
        "squadName",
        "coachId",
        "coachName"
    ]

    # check if coaches are blacklisted
    if coaches_blacklisted:
        order = [col for col in order if col not in ["coachId", "coachName"]]

    # add style of play names to order
    order += styles_definitions["styleOfPlayName"].to_list()

    # select columns
    squad_styles = squad_styles[order]

    # fix some column types
    squad_styles["matchId"] = squad_styles["matchId"].astype("Int64")
    squad_styles["competitionId"] = squad_styles["competitionId"].astype("Int64")
    squad_styles["iterationId"] = squad_styles["iterationId"].astype("Int64")
    squad_styles["matchDayIndex"] = squad_styles["matchDayIndex"].astype("Int64")
    squad_styles["squadId"] = squad_styles["squadId"].astype("Int64")
    squad_styles["wyscoutId"] = squad_styles["wyscoutId"].astype("Int64")
    squad_styles["heimSpielId"] = squad_styles["heimSpielId"].astype("Int64")
    squad_styles["skillCornerId"] = squad_styles["skillCornerId"].astype("Int64")
    squad_styles["optaId"] = squad_styles["optaId"].astype("string")
    squad_styles["statsPerformId"] = squad_styles["statsPerformId"].astype("string")
    squad_styles["transfermarktId"] = squad_styles["transfermarktId"].astype("string")
    squad_styles["soccerdonnaId"] = squad_styles["soccerdonnaId"].astype("string")
    squad_styles["dflId"] = squad_styles["dflId"].astype("string")

    # return data
    return squad_styles
