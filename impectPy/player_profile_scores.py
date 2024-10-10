# load packages
import pandas as pd
from impectPy.helpers import RateLimitedAPI
from .iterations import getIterations


######
#
# This function returns a pandas dataframe that contains all profile scores
# for a given iteration and a given set of positions per player
#
######

def getPlayerProfileScores(iteration: int, positions: list, token: str) -> pd.DataFrame:
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
    
    # get player profile scores per squad
    profile_scores_raw = pd.concat(
        map(lambda squadId: rate_limited_api.make_api_request_limited(
            url=f"https://api.impect.com/v5/customerapi/iterations/{iteration}/"
                f"squads/{squadId}/positions/{position_string}/player-profile-scores",
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
        url=f"https://api.impect.com/v5/customerapi/player-profiles",
        method="GET",
        headers=my_header
    ).process_response(
        endpoint="playerProfiles"
    )[["name"]]
    
    # get iterations
    iterations = getIterations(token=token, session=rate_limited_api.session)
    
    # unnest scorings
    profile_scores = profile_scores_raw.explode("profileScores").reset_index(drop=True)
    
    # unnest dictionary in kpis column
    profile_scores = pd.concat(
        [profile_scores.drop(["profileScores"], axis=1), pd.json_normalize(profile_scores["profileScores"])],
        axis=1
    )
    
    # merge with player scores to ensure all kpis are present
    profile_scores = profile_scores.merge(
        scores,
        left_on="profileName",
        right_on="name",
        how="outer",
        suffixes=("", "_right")
    )
    
    # get matchShares
    match_shares = profile_scores[
        ["iterationId", "squadId", "playerId", "positions", "playDuration", "matchShare"]].drop_duplicates()
    
    # fill missing values in the "name" column with a default value to ensure players without scorings don't get lost
    if len(profile_scores["name"][profile_scores["name"].isnull()]) > 0:
        profile_scores["name"] = profile_scores["name"].fillna("-1")
    
    # pivot kpi values
    profile_scores = pd.pivot_table(
        profile_scores,
        values="value",
        index=["iterationId", "squadId", "playerId", "positions"],
        columns="name",
        aggfunc="sum",
        fill_value=0,
        dropna=False
    ).reset_index()
    
    # drop "-1" column
    if "-1" in profile_scores.columns:
        profile_scores.drop(["-1"], inplace=True, axis=1)
    
    # merge with playDuration and matchShare
    profile_scores = profile_scores.merge(
        match_shares,
        left_on=["iterationId", "squadId", "playerId", "positions"],
        right_on=["iterationId", "squadId", "playerId", "positions"],
        how="inner",
        suffixes=("", "_right")
    )
    # merge with other data
    profile_scores = profile_scores.merge(
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
    profile_scores = profile_scores[profile_scores.iterationId.notnull()]
    
    # fix column types
    profile_scores["squadId"] = profile_scores["squadId"].astype(int)
    profile_scores["playerId"] = profile_scores["playerId"].astype(int)
    profile_scores["iterationId"] = profile_scores["iterationId"].astype(int)
    
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
    profile_scores = profile_scores[order]
    
    # return result
    return profile_scores