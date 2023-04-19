# impectPy <picture><source media="(prefers-color-scheme: dark)" srcset="https://github.com/ImpectAPI/logos/blob/main/impectPy_white.svg"><source media="(prefers-color-scheme: light)" srcset="https://github.com/ImpectAPI/logos/blob/main/impectPy_black.svg"><img alt="ImpectPy Logo" src="https://github.com/ImpectAPI/logos/blob/main/impectPy_black.svg" align="right" height="40"></picture>

A package provided by: Impect GmbH

Updated: April 17th 2023

The goal of the impectPy package is to provide an easy way for Impect
Customers to access data from the customer API. This API includes basic
information about competitions, competition iterations, and matches as
well as event data and aggregated scorings per player and position on
match level.

## Installation

You can install the developmental version of impectPy from
[GitHub](https://github.com/) with:

``` cmd
pip install git+https://github.com/ImpectAPI/impectPy.git
```

## Getting started

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
token = ip.getAccessToken(username = username, password = password)
```

This access token is a requirement to use any of the functions that
requests data from the API. We recommend to first get a list of
competition iterations that are enabled for your account.

``` python
# get list of competition iterations
competitions = ip.getCompetitions(token = token)

# print competition iterations to console
competitions
```

If any competition iteration you were expected to see is not listed,
please contact your sales representative. Now let’s assume you are
interested in data for 2022/23 season of the 1. Bundesliga
(competitionIteration = 518). The following snippet gets you a list of
matches for this competition and season:

``` python
# get match plan for competition iteration
matchplan = ip.getMatchplan(competitionIterationId = 518, token = token)

# print match to console
matchplan
```

The column `available` denotes whether a given match has been tagged by
Impect and the data is available to you. Let’s assume you are interested
in the FC Bayern München vs Borussia Dortmund game from April 1st 2023
(matchId = 84344). To request the event data for this game, run the
following code snippet:

``` python
# define match ID
matchId = 84344

# get event data for match
events = ip.getEventData(match = matchId, token = token)

# print first few rows from events dataframe to console
events.head()
```

You can access the aggregated scores per player and position for this
match in a similar way:

``` python
# define match ID
matchId = 84344

# get matchsums for match
matchsums = ip.getMatchsums(match = matchId, token = token)

# print first few rows from matchsums dataframe to console
matchsums.head()
```

In case you wish to retrieve data for multiple matches, we suggest using
the following method to do so. Let’s also get the event data for the RB
Leipzig vs FSV Mainz 05 game (matchId = 84350) from the same day:

``` python
# define list of matches
matches = [84344, 84350]

# apply getEventData function to a set of matchIds
events = pd.concat(
    map(lambda match_id: ip.getEventData(match=match_id, token=token), matches),
    ignore_index=True)

# apply getMatchsums function to a set of matchIds
matchsums = pd.concat(
    map(lambda match_id: ip.getMatchsums(match=match_id, token=token), matches),
    ignore_index=True)
```

Please keep in mind that Impect enforces a rate limit of 8 requests per
second per user. As the function usually runs for about 2 seconds, there
shouldn’t be any issues, but it might be a potential issue.

## SportsCodeXML

It is also possible to convert a dataframe containing event data into an XML file,
that can be imported into Sportscode. Let's use the Bayern vs Dortmund game from 
earlier as an example:

``` python
# define matchId
match_id = 84344

# get event data for matchId
events = ip.getEventData(match_id, token)

# define lead and lag time in seconds
lead = 3
lag = 3

# define period start offsets from video start in seconds
p1Start = 16
p2Start = 48*60 + 53
p3Start = 0 # set to 0 if there was no extra time
p4Start = 0 # set to 0 if there was no extra time

# generate xml
xml_tree = ip.generateXML(events=events,
                          lead=lead,
                          lag=lag,
                          p1Start=p1Start,
                          p2Start=p2Start,
                          p3Start=p3Start,
                          p4Start=p4Start)

# write to xml file 
with open(f"match{match_id}_"
          # add home team name
          f"{events.squadHomeName.unique().tolist()[0].replace(' ', '_')}"
          f"_vs_"
          # add away team name
          f"{events.squadAwayName.unique().tolist()[0].replace(' ', '_')}"
          f".xml",
          "wb") as file:
    xml_tree.write(file,
               xml_declaration=True,
               encoding='utf-8',
               method="xml")
```

If you wish to customize the XML file in terms of included KPIs or the applied
grouping, please refer to the "define parameters" section of
scripts/generateCustomXML.py

## Final Notes

Further documentation on the data and explanations of variables can be
found in our [glossary](https://glossary.impect.com/).
