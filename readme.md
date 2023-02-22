Some hobby quality python scripts to get and analyze NHL data from their public API's.  Not affiliated with the NHL.

This repo only contains code/scripts.  All data is saved locally and those paths are in the .gitignore so they aren't saved to the repo.

## Approach and key assumptions
1. If the API call result is for an event in the past it is unlikely to change and should be cached without expiring
2. Current (live) or future scheduled events do change until the game status is Final.  Don't cache those.
3. Normalize and flatten the json results and save them to a sqlite database locally
3.1. Cache hits indicate already processed results; don't insert those into the database again.  # TODO: How much does this matter?  
3.2. Do insert cache misses because data may have changed since the last run
4. Publish the sqlite database using datasette on fly.io


### Details
1. Fetch season data from API
1. For the current season, cache for a few minutes.  For older seasons, cache without expiring
1. [one time] Scan all game data and get the unique json keys.  Transform the keys to column names and create database table.
1. Insert or replace existing rows by game ID into database table
1. Pass the list of game ID's with state to the feed live module
1. Fetch feed live data from API for each game ID
1. For games with state == 'Final', never expire from the cache.  Else expire immediately.
1. [one time] Scan all feed live data and get the unique json keys.  Transform the keys to column names and create database table.
1. Insert or replace existing rows by game ID and event ID into database table



## standings_change_analysis.py
Compare each team's standings to the previous season, and calculate the change in points and the change in the standings position year-over-year.  The calculations for the 2001-2021 seasons are saved to the file `final_result.csv`
* Only includes regular season games
* No special handling for seasons impacted by lockouts, COVID, etc.

### Uses for the data
 * Understand how many places a team is likely to move in the standings from one season to the next. 

### Known issues
* The 2004 season was cancelled because of a labor dispute.  The data has not been reviewed or corrected for this.  The 2005 season should be compared to the 2003 season.

## Credits
* Kyle Pastor for the inspiration to fetch and analyze data from the NHL API's: https://towardsdatascience.com/nhl-analytics-with-python-6390c5d3206d
* Drew Hynes for documenting the NHL data API's here: https://gitlab.com/dword4/nhlapi/-/tree/master
* Thanks to the NHL for making the data available.  All data returned by the API is owned and copyrighted by the NHL and it includes the notice: "NHL and the NHL Shield are registered trademarks of the National Hockey League. NHL and NHL team marks are the property of the NHL and its teams. © NHL 2022. All Rights Reserved."