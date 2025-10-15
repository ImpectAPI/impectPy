# impectPy 2.5.0

## Major Changes
* Use new endpoints to drastically improve performance of `getPlayerMatchScores()` and `getPlayerIterationScores()`. The argument `positions` is no longer required. If it is not spplied the function defaults to the new endpoints and returns all unique player-position-squad combinations.
* Add coaches ot the following functions:
  * `getEvents()`
  * `getPlayerMatchSums()`
  * `getSquadMatchSums()`
  * `getPlayerMatchScores()`
  * `getSquadMatchScores()`
* Add function `getSquadCoefficients()` to retrieve detailed model coefficients to enable match predictions

## Minor Changes
* Fix error in `getPlayerIterationAverages()` regarding type conversions
* Use `NA` as fill value instead of 0 for score related functions
* Minor fixes to enable PyPi submission
* Improve error handling

# impectPy 2.4.5

## Minor Changes
* fix bug in `getPlayerIterationAverages()`function

# impectPy 2.4.4

## Major Changes
* Rename function `generateSportsCodeXML()` to `generateXML()`
* Add proper xml structure to the `generateXML()` function for Python versions >= 3.9
* Significantly improve customization options for new `generateXML()` function with new function arguments
  * `kpis`:Customize KPIs included
  * `lables`: Customize labels included
  * `codeTag`: Customize code tag selection
  * `labelSorting`: Enable/Disable label sorting

## Minor Changes
* fix bug in `getEvents()` that prevented the column `duelPlayerName`from being populated correctly

# impectPy 2.4.3

## Minor Changes
* Add FIFA Country Name to the following functions
  * `getIterations()`
  * `getPlayerMatchsums()`
  * `getPlayerIterationAverages()`
  * `getPlayerMatchScores()`
  * `getPlayerIterationScores()`
  * `getPlayerProfileScores()`
* Fix bug in `getStartingPositions()` that resulted from players not having a shirt number assigned

# impectPy 2.4.2

## Minor Changes
* Improvements to `getSubstitutions()` to handle matches where one team did not substitute any players
* Significant performance improvements to `getPlayerIterationAverages()`

# impectPy 2.4.1

## Minor Changes
* Fix error in `getEvents()` that prevented set piece data from properly being joined to event data
* Fix error in `getSubstitutions()` & `getStartingPositions()` that caused an error when players switched shirt numbers

# impectPy 2.4.0

## Major Changes
* Add function `getFormations()` to retrieve squad formations on match level
* Add function `getStartingPositions()` to retrieve squad starting positions on match level
* Add function `getSubstitutions()` to retrieve squad substitutions on match level

## Minor changes
* Add IMPECT class to enable object-oriented API usage and improve performance
* Add new arguments to `getSportCcodeXML()` to enable more customization options for the generated XML:
  * Disable sequencing
  * Disable KPI buckets

# impectPy 2.3.1

## Major Changes
* Add function `getSquadRatings()` to retrieve squad ratings

## Minor changes
* Add attribute `inferredSetPiece`to `getEvents()` function
* Add ID mappings to other providers (HeimSpiel, SkillCorner, Wyscout) to several functions
* Fix bug in `getSquadMatchScores()` that occured if the home team did not have a player at the given position

# impectPy 2.3.0

## Major changes
* Add new `getSetPieces()` function
* Add set piece data to `getEvents()`
* Add arguments to `getEvents()` function that control the addition of KPIs and set piece data to the events dataframe

## Minor changes
* Fix error in `getEvents()` for matches without any tagged duels
* Use raw string notation when using regex to clean column names
* Add EventId to XML generation
* Fix error in `getPlayerIterationScores()`, `getPlayerIterationScores()` & `getPlayerProfileScores()` when no records are returned for given combination of match/iteration and position

# impectPy 2.2.0

## Major changes
* add new functions to query the new customer API endpoints that provide ratios & scores

## Minor changes
* switch from German country name to FIFA country name
* Update to readme structure

# impectPy 2.1.0

## Major changes
* add new attributes from dataVersion V4 to `getEvents()`

## Minor changes
* add some of the new dataVersion V4 attributes to `generateSportsCodeXML()`
* fix labels of periods in `generateSportsCodeXML()` to better support MatchTracker integration

# impectPy 2.0.6

## Minor changes
* add new label to player phase of xml export: team

# impectPy 2.0.4

# impectPy 2.0.5

## Minor changes
* add more player master data to `getPlayerMatchsums()` and `getPlayerIterationAverages()`
* fix issue with several functions that occurred with pandas version 2.1 or newer
* fix minor consistency issue in code for `generateSportsCodeXML()`
* edit naming of kickoff events in `generateSportsCodeXML()` to properly support SBG MatchTracker

# impectPy 2.0.4

## Minor changes
* fix bug in `getSquadMatchsums()` and `getPlayerMatchsums()` caused by duplicates
* fix bug in `getMatches()` function caused by addition of wyscoutIds
* 
* improve error handling for functions that use match ids as input
* improve error handling for `getMatches()` function
* add `playDuration` on player level to `getSquadMatchsums()`, `getPlayerMatchsums()`, `getPlayerIterationAverages()` and `getSquadIterationAverages()`
* fix bug in `getEvents()`, `getSquadMatchsums()`, `getPlayerMatchsums()`, `getPlayerIterationAverages()` and `getSquadIterationAverages()` that was caused by the addition of several new keys to the KPI endpoint

# impectPy 2.0.3

## Minor changes
* fix bug in `getEvents()` function caused by querying data for multiple iterations of the same competition

# impectPy 2.0.2

## Minor changes
* fix bug in `getPlayerIterationAverages()` function caused by user access rights
* fix bug in `getIterations()` function caused by addition of wyscoutIds
* fix bug in `getMatches()` function caused by addition of wyscoutIds

# impectPy 2.0.1

## Minor changes
* fix bug in `getSquadIterationAverages()` function
* fix bug in `getEvents()` function
* fix bug in `generateSportsCodeXML()` function
* fix bug in `getPlayerMatchsums()` function
* add sorting by id to `getIterations()` function
* add sorting by id to `getMatches()` function
* fix function argument name in readMe

# impectPy 2.0.0

## Major changes
* Modify package to support the IMPECT API V5 instead of V4
* Add `getPlayerIterationAverages()` function
* Add `getSquadIterationAverages()` function

## Minor changes
* Fix error in readme sample code
* raise exception for wrong `matches` argument input type in several functions

# impectPy 1.0.3

## Minor changes
* fix bug in `generateSportsCodeXML()` that did not filter out events of action type 'NO_VIDEO_AVAILABLE', 'FINAL_WHISTLE' or 'REFEREE_INTERCEPTION' correctly
* fix bug in `generateSportsCodeXML()` that caused certain kickoffs to be missing

# impectPy 1.0.2

## Minor changes
* add features and KPIs to `generateSportsCodeXML()` function, finalize initial built for IMPECT portals  

# impectPy 1.0.1

## Minor improvements and bug fixes
* Fix issue in `getAccessToken()` with certain characters in password

# impectPy 1.0.0

## Major changes
* Release package

## Minor changes
* implement retry on HTTP response codes other than 200

# impectPy 0.1.1

## Minor improvements and bug fixes
* renamed `generateXML()` to `generateSportsCodeXML()`
* Minor bug fixes in `generateSportsCodeXML()`

# impectPy 0.1

## Major changes
* Added basic package build
* Added `getAccessToken()` function
* Added `getCompetitions()` function
* Added `getMatchplan()` function
* Added `getEventData()` function
* Added `getMatchsums()` function

## Minor improvements and bug fixes
* Added a `NEWS.md` file to track changes to the package
* Added `README.md`
* Added `LICENSE.md`