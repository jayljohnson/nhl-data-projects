Some hobby quality python scripts to get and analyze NHL data from their public API's.  Not affiliated with the NHL.

## get_game_linescores.py
Get the game linescore data for every game across a range of seasons, and save the results for each season to a separate file.  The files are saved in python's pickle binary format.

## standings_change_analysis.py
Compare each team's standings to the previous season, and calculate the change in points and the change in the standings position year-over-year.  The calculations for the 2001-2021 seasons are saved to the file `final_result.csv`
* Only includes regular season games
* No special handling for seasons impacted by lockouts, COVID, etc.

### Uses for the data
 * Understand how many places a team is likely to move in the standings from one season to the next. 

### Schema
* season: The year the season started (e.g. 2021)
* season_formatted: The year the season started along with the last two digits of the year the season ended (e.g. 2021-22)
* team: The team name
* points: The number of points from regulation games
* previous_season_points: The number of points from the previous seasons
* change_in_points: The change in points from the previous steason
* place: The final position in the standings
* previous_season_place: The final position in the standings from the previous season
* change_in_place: How many positions the team moved up (positive) or down (negative) compared to the previous season

### Known issues
* The 2004 season was cancelled because of a labor dispute.  The data has not been reviewed or corrected for this.  The 2005 season should be compared to the 2003 season.

## Credits
* Kyle Pastor for the inspiration to fetch and analyze data from the NHL API's: https://towardsdatascience.com/nhl-analytics-with-python-6390c5d3206d
* Drew Hynes for documenting the NHL data API's here: https://gitlab.com/dword4/nhlapi/-/tree/master
* Thanks to the NHL for making the data available.  All data returned by the API is owned and copyrighted by the NHL and it includes the notice: "NHL and the NHL Shield are registered trademarks of the National Hockey League. NHL and NHL team marks are the property of the NHL and its teams. © NHL 2022. All Rights Reserved."

"""
# Ideas:

## Current dataset
* Distance from home city and away city
* Consecutive home/away games

## Needs new datasets
* Ice time by forward line pairings, top 3 pairings
* Blocked shots
* Goalie pulled at game end

## General learning
* What is CORSI, how is it calculated?
"""