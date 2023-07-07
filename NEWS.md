# impectPy 2.0.1

## Minor changes
* Fix error in readme sample code
* raise exception for wrong `matches` argument input type
* fix error in base urls for most functions

# impectPy 2.0.0

## Major changes
* Modify package to support the IMPECT API V5 instead of V4
* Add `getPlayerIterationAverages()` function
* Add `getSquadIterationAverages()` function
* 
* # impectPy 1.0.3

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
