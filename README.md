# impectPy <picture><source media="(prefers-color-scheme: dark)" srcset="https://github.com/ImpectAPI/logos/blob/main/impectPy_white.svg"><source media="(prefers-color-scheme: light)" srcset="https://github.com/ImpectAPI/logos/blob/main/impectPy_black.svg"><img alt="ImpectPy Logo" src="https://github.com/ImpectAPI/logos/blob/main/impectPy_black.svg" align="right" height="40"></picture>

A package provided by: Impect GmbH

Version: v2.2.0

**Updated: October 17th 2024**

---

**Supported API Version: V5**<br>
For older versions, please see list below:

- API V4: https://github.com/ImpectAPI/impectPy/tree/v1.0.3
- API V3: not supported by this package

---

## Introduction

The goal of the impectPy package is to provide an easy way for Impect
Customers to access data from the customer API. This API includes basic
information about competitions, competition iterations, and matches as
well as event data and aggregated scorings per player and position on
match and season level.

## Installation

You can install the latest version of impectPy from
[GitHub](https://github.com/) with:

``` cmd
pip install git+https://github.com/ImpectAPI/impectPy.git@v2.2.0
```

## Usage

### Getting started

Before accessing any data via our API, you will need to request a bearer
token for authorization. You can get this authorization token using the
following code snippet:

``` python
import impectPy as ip
import pandas as pd

# define login credentials
username = "yourUsername"
password = "yourPassword"

# get access token
token = ip.getAccessToken(username=username, password=password)
```

This access token is a requirement to use any of the functions that
requests data from the API. We recommend to first get a list of
competition iterations that are enabled for your account.

### Retrieve Basic Information

``` python
# get list of iterations
iterations = ip.getIterations(token=token)

# print iterations to console
iterations
```

If any iteration you were expected to see is not listed, please contact
your sales representative. Now let’s assume you are interested in data
for 2022/23 season of the 1. Bundesliga (iteration = 518). The following
snippet gets you a list of matches for this iteration:

``` python
# get matches for iteration
matchplan = ip.getMatches(iteration=518, token=token)

# print matches to console
matchplan
```

The column `available` denotes whether a given match has been tagged by Impect
and the data is available to you.

### Retrieve Match Level Data

Let's assume you are interested in the FC
Bayern München vs Borussia Dortmund game from April 1st 2023 (matchId = 84344).
As the function allow for multiple games to be requested at once, we need to wrap
the matchId into a list. Hence, to request the event data for this game, run the
following code snippet:

``` python
# define matches to get event data for
matches = [84344]

# get event data for matches
events = ip.getEvents(
    matches=matches, 
    token=token,
    include_kpis=True,
    include_set_pieces=True
)

# print first few rows from events dataframe to console
events.head()
```

You can access the aggregated scores per player and position or per
squad for this match in a similar way. You can also find more detailed data
around set piece situations within our API.
Also, we provide you with IMPECT scores and ratios that you might know from our 
Scouting and Analysis portals. On player level, these are calculated across 
positions which is why you have to supply the function with a list of positions 
your want to retrieve data for:

``` python
# define matches to get further data for
matches = [84344]

# get set piece data including KPI aggregates
setPieces = ip.getSetPieces(matches=matches, token=token)

# get kpi matchsums for match per player and position
playerMatchsums = ip.getPlayerMatchsums(matches=matches, token=token)

# get kpi matchsums for match per squad
squadMatchsums = ip.getSquadMatchsums(matches=matches, token=token)

# define positions to get scores aggregated by
positions = ["LEFT_WINGBACK_DEFENDER", "RIGHT_WINGBACK_DEFENDER"]

# get player scores and ratios for match and positions per player
playerMatchScores = ip.getPlayerMatchScores(
    matches=matches,
    positions=positions,
    token=token
)

# get squad scores and ratios for match per squad
squadMatchScores = ip.getSquadMatchScores(matches=matches, token=token)
```

In case you wish to retrieve data for multiple matches, we suggest using
the following method to do so in order to minimize the amount of
requests sent to the API. Let’s also get the event data for the RB
Leipzig vs FSV Mainz 05 game (matchId = 84350) from the same day:

``` python
# define list of matches
matches = [84344, 84350]

# apply getEvents function to a set of matchIds
events = ip.getEvents(
    matches=matches, 
    token=token,
    include_kpis=True,
    include_set_pieces=True
)

# get set piece data including KPI aggregates
setPieces = ip.getSetPieces(matches=matches, token=token)

# get matchsums for matches per player and position
playerMatchsums = ip.getPlayerMatchsums(matches=matches, token=token)

# get matchsums for matches per squad
squadMatchsums = ip.getSquadMatchsums(matches=matches, token=token)

# define positions to get scores aggregated by
positions = ["LEFT_WINGBACK_DEFENDER", "RIGHT_WINGBACK_DEFENDER"]

# get player scores and ratios for match and positions per player
playerMatchScores = ip.getPlayerMatchScores(
    matches=matches,
    positions=positions,
    token=token
)

# get squad scores and ratios for match per squad
squadMatchScores = ip.getSquadMatchScores(matches=matches, token=token)
```

### Retrieve Iteration Level Data

Starting from API version V5, we also offer an endpoint to get KPI average values
per iteration on player as well as squad level. These averages are calculated by
dividing the kpi sum of all individual matches by the sum of matchShares the player
accumulated at a given position. On a team level we divide the score by the
amount of matches played by the team.
Also, we provide you with IMPECT scores and ratios that you might know from our 
Scouting and Analysis portals. On player level, these are calculated across 
positions which is why you have to supply the function with a list of positions 
your want to retrieve data for.
Let's assume you were interested in wingbacks in the 2022/2023 Bundesliga season, 
then you could use this code snippet:

``` python
# define iteration ID
iteration = 518

# define positions to get scores aggregated by
positions = ["LEFT_WINGBACK_DEFENDER", "RIGHT_WINGBACK_DEFENDER"]

# get player kpi averages for iteration
playerIterationAverages = ip.getPlayerIterationAverages(
    iteration=iteration,
    token=token
)

# get squad kpi averages for iteration
squadIterationAverages = ip.getSquadIterationAverages(
    iteration=iteration,
    token=token
)

# get player scores and ratios for iteration and positions
playerIterationScores = ip.getPlayerIterationScores(
    iteration=iteration,
    positions=positions,
    token=token
)

# get squad scores and ratios for iteration
squadIterationScores = ip.getSquadIterationScores(
    iteration=iteration,
    token=token
)
```

You can now also retrieve the positional profile scores for players via our API. This 
includes profiles that you created through the scouting portal. The function requires a 
positional input that determines which matchShares to consider when computing the scores. 
In the below example, all matchShares that a player played as either a left back or a right 
back are included for profile score calculation.

``` python
# define iteration ID
iteration = 518

# define positions to get scores aggregated by
positions = ["LEFT_WINGBACK_DEFENDER", "RIGHT_WINGBACK_DEFENDER"]

# get player profile scores
playerProfileScores = getPlayerProfileScores(iteration, positions, token)
```

Please keep in mind that Impect enforces a rate limit of 10 requests per second
per user. A token bucket logic has been implemented to restrict the amount of API
calls made on the client side already. The rate limit is read from the first limit
policy sent back by the API, so if this limit increases over time, this package will
act accordingly.

### SportsCodeXML

It is also possible to convert a dataframe containing event data into an XML file,
that can be imported into Sportscode. Please make sure to only retrieve event data for
one game at a time. Let's use the Bayern vs Dortmund game from earlier as an example:

``` python
# define matchId
matches = [84344]

# get event data for matchId
events = ip.getEvents(matches=matches, token=token)

# define lead and lag time in seconds
lead = 3
lag = 3

# define period start offsets from video start in seconds
p1Start = 16 # first half kickoff happens after 16 seconds in your video file
p2Start = 48 * 60 + 53 # first half kickoff happens after 48 minutes and 53 seconds in your video file
p3Start = 0 # set to timestamp of the kickoff of the first half of extra time
p4Start = 0 # set to timestamp of the kickoff of the second half of extra time
p5Start = 0 # set to timestamp of the  of the penalty shootout
kickoff
# generate xml
xml_tree = ip.generateSportsCodeXML(
    events=events,
    lead=lead,
    lag=lag,
    p1Start=p1Start,
    p2Start=p2Start,
    p3Start=p3Start,
    p4Start=p4Start,
    p5Start=p5Start
)

# write to xml file
with open(f"match{matches[0]}_"
          # add home team name
          f"{events.homeSquadName.unique().tolist()[0].replace(' ', '_')}"
          f"_vs_"
          # add away team name
          f"{events.awaySquadName.unique().tolist()[0].replace(' ', '_')}"
          f".xml",
          "wb") as file:
    xml_tree.write(file,
                   xml_declaration=True,
                   encoding='utf-8',
                   method="xml")
```

## Final Notes

Further documentation on the data and explanations of variables can be
found in our [glossary](https://glossary.impect.com/).
