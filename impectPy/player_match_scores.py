# load packages
import pandas as pd
import requests
import warnings
from impectPy.helpers import RateLimitedAPI, unnest_mappings_df, ForbiddenError, safe_execute
from .matches import getMatchesFromHost
from .iterations import getIterationsFromHost

# define the allowed positions
allowed_positions = [
  "GOALKEEPER",
  "LEFT_WINGBACK_DEFENDER",
  "RIGHT_WINGBACK_DEFENDER",
  "CENTRAL_DEFENDER",
  "DEFENSE_MIDFIELD",
  "CENTRAL_MIDFIELD",
  "ATTACKING_MIDFIELD",
  "LEFT_WINGER",
  "RIGHT_WINGER",
  "CENTER_FORWARD"
]

######
#
# This function returns a pandas dataframe that contains all scores for a
# given match and a given set of positions aggregated per player
#
######


def getPlayerMatchScores(
        matches: list, token: str, positions: list = None, session: requests.Session = requests.Session()
) -> pd.DataFrame:

    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getPlayerMatchScoresFromHost(matches, connection, "https://api.impect.com", positions)

def getPlayerMatchScoresFromHost(matches: list, connection: RateLimitedAPI, host: str, positions: list = None) -> pd.DataFrame:

    # check input for matches argument
    if not isinstance(matches, list):
        raise Exception("Argument 'matches' must be a list of integers.")

    # check input for positions argument
    if not isinstance(positions, list) and positions is not None:
        raise Exception("Input for positions argument must be a list")

    # check if the input positions are valid
    if positions is not None:
        invalid_positions = [position for position in positions if position not in allowed_positions]
        if len(invalid_positions) > 0:
            raise Exception(
                f"Invalid position(s): {', '.join(invalid_positions)}."
                f"\nChoose one or more of: {', '.join(allowed_positions)}"
            )

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
    def fetch_player_match_scores(connection, url):
        return connection.make_api_request_limited(
            url=url,
            method="GET"
        ).process_response(endpoint="Player Match Scores")

    # get player scores
    if positions is None:

        # create list to store dfs
        scores_list = []
        for match in matches:
            scores = safe_execute(
                fetch_player_match_scores,
                connection,
                url=f"{host}/v5/customerapi/matches/{match}/player-scores",
                identifier=f"{match}",
                forbidden_list=forbidden_matches
            ).assign(matchId=match)
            scores_list.append(scores)
        scores_raw = pd.concat(scores_list).reset_index(drop=True).reset_index(drop=True)

    else:

        # compile list of positions
        position_string = ",".join(positions)

        # create list to store dfs
        scores_list = []
        for match in matches:
            scores = safe_execute(
                fetch_player_match_scores,
                connection,
                url=f"{host}/v5/customerapi/matches/{match}/positions/{position_string}/player-scores",
                identifier=f"{match}",
                forbidden_list=forbidden_matches
            ).assign(
                matchId=match,
                positions=position_string
            )
            scores_list.append(scores)
        scores_raw = pd.concat(scores_list).reset_index(drop=True).reset_index(drop=True)

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

    # get player scores
    scores = connection.make_api_request_limited(
        url=f"{host}/v5/customerapi/player-scores",
        method="GET"
    ).process_response(
        endpoint="Player Iteration Scores"
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

    # create empty df to store player scores
    player_scores = pd.DataFrame()

    # manipulate player_scores

    # iterate over matches
    for i in range(len(scores_raw)):

        # create empty df to store per match scores
        match_player_scores = pd.DataFrame()

        # iterate over sides
        for side in ["squadHomePlayers", "squadAwayPlayers"]:

            # get data for index
            temp = scores_raw[side].loc[i]

            # check if any records for side at given position
            if len(temp) == 0:
                continue

            # convert to pandas df
            if positions is None:
                temp = pd.DataFrame(temp).assign(
                    matchId=scores_raw.matchId.loc[i],
                    squadId=scores_raw[side.replace("Players", "Id")].loc[i],
                )

                # extract matchshares
                matchshares = temp[["matchId", "squadId", "id", "matchShare", "playDuration", "position"]].drop_duplicates()

            else:
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
            if positions is None:
                temp = pd.pivot_table(
                    temp,
                    values="value",
                    index=["matchId", "squadId", "position", "id"],
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
            else:
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

            # append to match_player_scores
            match_player_scores = pd.concat([match_player_scores, temp])

        # check if any records for match at given position
        if len(match_player_scores) == 0:
            print(f"No players played at given position in match {scores_raw.loc[i].matchId}")

        # append to player_scores
        player_scores = pd.concat([player_scores, match_player_scores])

    # check if any records for any match at given position
    if len(player_scores) == 0:
            raise Exception("No players played at given positions for any given match. Execution stopped.")

    # merge with other data
    player_scores["squadName"] = player_scores.squadId.map(squad_map)
    player_scores["playerCountry"] = player_scores.squadId.map(country_map)
    player_scores = player_scores.merge(
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
        player_scores["coachId"] = player_scores["coachId"].astype("Int64")
        player_scores["coachName"] = player_scores.coachId.map(coaches_map)

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
        "positions" if positions is not None else "position",
        "matchShare",
        "playDuration",
    ]

    # add kpiNames to order
    order += scores["name"].to_list()

    # check if coaches are blacklisted
    if coaches_blacklisted:
        order = [col for col in order if col not in ["coachId", "coachName"]]

    # select columns
    player_scores = player_scores[order]

    # fix some column types
    player_scores["matchId"] = player_scores["matchId"].astype("Int64")
    player_scores["squadId"] = player_scores["squadId"].astype("Int64")
    player_scores["playerId"] = player_scores["playerId"].astype("Int64")
    player_scores["wyscoutId"] = player_scores["wyscoutId"].astype("Int64")
    player_scores["heimSpielId"] = player_scores["heimSpielId"].astype("Int64")
    player_scores["skillCornerId"] = player_scores["skillCornerId"].astype("Int64")

    # return data
    return player_scores