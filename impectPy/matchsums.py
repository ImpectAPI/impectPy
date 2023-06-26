######
#
# This function returns a pandas dataframe that contains all events for a
# given match
#
######


# load packages
import requests
import pandas as pd
import time
from .helpers import make_api_request


# define function
def getMatchsums(match: str, token: str) -> pd.DataFrame:
    # construct header with access token
    my_header = {"Authorization": f"Bearer {token}"}

    # create session object
    with requests.Session() as session:
        # get match info
        response = make_api_request(url=f"https://api.impect.com/v4/customerapi/matches/{match}",
                                    method="GET",
                                    headers=my_header,
                                    session=session)

        # check response status
        response.raise_for_status()

        # get data from response
        match_info = response.json()["data"]

        # define function to extract players
        def extract_players(dict, side):
            # convert to pandas df
            df = pd.json_normalize(dict,
                                   record_path=[side, "players"],
                                   meta=["matchId", "date", "dateTime", "competition"])

            # filter for players with playtime only
            df = df.loc[df.playTime.str.len() > 0]

            # add columns for squadId and squadName
            df["squadId"] = dict[side]["squadId"]
            df["squadName"] = dict[side]["name"]

            # apply json_normalize to unnest the 'competition' column and reorder columns
            df = pd.concat([df[["matchId", "date", "dateTime"]],
                            df["competition"].apply(pd.Series),
                            df[["squadId", "squadName", "playerId", "commonname", "playTime"]]],
                           axis=1)

            # return df
            return df

        # apply function to both sides
        players_home = extract_players(match_info, "squadHome")
        players_away = extract_players(match_info, "squadAway")

        # merge home and away players to one list
        players = pd.concat([players_home, players_away])

        # explode playTime column
        players = players.explode("playTime")

        # unnest dictionary in playTime column
        players = pd.concat([players.drop(["playTime"], axis=1),
                             players["playTime"].apply(pd.Series)],
                            axis=1)

        # get match sums
        response = make_api_request(url=f"https://api.impect.com/v4/customerapi/matches/{match}/matchsums",
                                    method="GET",
                                    headers=my_header,
                                    session=session)

        # check response status
        response.raise_for_status()

        # get data from response
        match_sums = response.json()["data"]

        # get kpi list
        kpis = make_api_request(url=f"https://api.impect.com/v4/customerapi/kpis",
                                method="GET",
                                headers=my_header,
                                session=session)

        # check response status
        kpis.raise_for_status()

        # get data from response
        kpis = kpis.json()["data"]

        # convert to df
        kpis = pd.DataFrame(kpis)

        # define function to extract matchsums
        def extract_matchsums(dict, side):
            # normalize dictionary into dataframe
            match_sums = pd.json_normalize(dict, record_path=[side, "players"])

            # filter for players with playtime only
            match_sums = match_sums.loc[match_sums.scorings.str.len() > 0]

            # explode playTime column
            match_sums = match_sums.explode("scorings")

            # unnest dictionary in playTime column
            match_sums = pd.concat([match_sums.drop(["scorings"], axis=1),
                                    match_sums["scorings"].apply(pd.Series)],
                                   axis=1)

            # merge with kpis to ensure all kpis are present
            match_sums = pd.merge(match_sums, kpis, on="kpiId", how="outer")

            # pivot data
            match_sums = pd.pivot_table(match_sums,
                                        values="totalValue",
                                        index=["playerId", "detailedPosition"],
                                        columns="kpiId",
                                        aggfunc="sum",
                                        fill_value=0,
                                        dropna=False)

            # return data
            return match_sums

        # apply function to both sides
        match_sums_home = extract_matchsums(match_sums, "squadHome")
        match_sums_away = extract_matchsums(match_sums, "squadAway")

        # merge home and away players to one list
        match_sums = pd.concat([match_sums_home, match_sums_away])

        # merge dfs
        data = pd.merge(players.reset_index(),
                        match_sums.reset_index(),
                        how="left",
                        left_on=["playerId", "detailedPosition"],
                        right_on=["playerId", "detailedPosition"])

        # create dict to fix kpi names
        names_fix = {row.kpiId: row.kpiName for index, row in kpis.iterrows()}

        # use the rename method to replace the column names
        data = data.rename(columns=names_fix)

        # drop index column
        data = data.drop("index", axis=1)

        # return data
        return data