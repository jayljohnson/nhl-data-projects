These scripts are free to use or modify but the data is 

## get_game_status_by_year.py
Call the game linescore API for every game across a range of seasons, and save the results for each season to a separate file.

## calculate_historical_standings_change.py
From the saved linescore files of each season, compare each team's standings to the previous season and calculate the change in points and the change in the standings position year-over-year.  The output is saved to the file `final_result.csv`