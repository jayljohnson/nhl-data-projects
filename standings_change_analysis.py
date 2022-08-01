from collections import defaultdict
import pickle

seasons = range(2000, 2022)
standings = {}
rank = {}
game_results = []
game_results_cumulative_points = []
game_count_by_season = {}

def get_current_standings(team_points):
    return {k: v for k, v in sorted(team_points.items(), key=lambda item: item[1], reverse=True)}

def get_rank(current_standings):
    result = {}
    previous = None
    for i, (team, position) in enumerate(current_standings.items()):
        if position != previous:
            rank, previous = i + 1, position
        result[team] = rank
    return result

for season in seasons:
    game_count = 0
    team_points = defaultdict(lambda: 0)

    with open(f'./{season}-linescore.pkl', 'rb') as f:
        games = pickle.load(f)
        for i, game in enumerate(games):
            if game.get("messageNumber") != 2:
                previous_standings = get_current_standings(team_points)
                previous_rank = get_rank(previous_standings)

                # team.goaliePulled is always False and team.numSkaters doesn't make sense.  why?
                # It would be interesting to see stats on pulled goalie conversion to goals scored.
                game_count += 1
                game_data = {}
                game_data["season"] = season
                game_id = f"{season}02{str(i).zfill(4)}"
                game_data["game_id"] = game_id

                teams = game.get("teams")

                home_team = teams.get("home")

                home_team_name = home_team["team"]["name"]
                game_data["home_team_id"] = home_team["team"]["id"]
                game_data["home_team"] = home_team_name
                home_team_goals = home_team["goals"]
                game_data["home_goals"] = home_team_goals
                game_data["home_shots_on_goal"] = home_team["shotsOnGoal"]

                away_team = teams.get("away")

                away_team_name = away_team["team"]["name"]
                game_data["away_team_id"] = away_team["team"]["id"]
                game_data["away_team"] = away_team_name
                away_team_goals = away_team["goals"]
                game_data["away_goals"] = away_team_goals
                game_data["away_shots_on_goal"] = away_team["shotsOnGoal"]

                # Calculate the points for standings
                # Final period key: regulation = 3, overtime = 4, shootout = 5, cancelled = 0
                final_period = game.get("currentPeriod")
                if final_period == 3:
                    game_data["home_points"], game_data["away_points"] = (2, 0) if home_team_goals > away_team_goals else (0, 2) 
                    game_data["game_finish_status"] = "regulation"
                elif final_period == 4:
                    # Starting from 1999-00, an overtime loss earns 1 point for the losing team 
                    if season >= 1999:
                        if home_team_goals != away_team_goals:
                            game_data["home_points"], game_data["away_points"] = (2, 1) if home_team_goals > away_team_goals else (1, 2)
                            game_data["game_finish_status"] = "overtime win"
                        # Before shootouts were introduced in 2005-06, both team get 1 point for games tied after overtime.  
                        # Those games ended in period 4.
                        elif home_team_goals == away_team_goals:
                            game_data["home_points"], game_data["away_points"] = (1, 1)
                            game_data["game_finish_status"] = "overtime tie"
                        else:
                            raise Exception("Unhandled scenario for period 4")
                    # Before 1999-00, an overtime loss earned 0 points for the losing team 
                    else:
                        game_data["home_points"], game_data["away_points"]  = (2, 0) if home_team_goals > away_team_goals else (0, 2)
                        game_data["game_finish_status"] = "overtime win"
                # Starting from 2005-06, games tied after overtime go to a shootout
                # Before then, regular season games ended after one OT (final_period = 4)
                elif final_period == 5:
                    game_data["home_points"], game_data["away_points"] = (2, 1)  if home_team_goals > away_team_goals else (1, 2)
                    game_data["game_finish_status"] = "shootout win"
                elif final_period == 0:
                    game_data["home_points"], game_data["away_points"] = (0, 0)
                    game_data["game_finish_status"] = "cancellation"
                else:
                    raise Exception("Unhandled game points scenario")

                # Get the cumulative points for the standings and game results
                team_points[home_team_name] += game_data["home_points"]
                game_data["home_team_cumulative_points"] = game_data.get("home_team_cumulative_points", 0) + team_points[home_team_name]
                team_points[away_team_name] += game_data["away_points"]
                game_data["away_team_cumulative_points"] = game_data.get("away_team_cumulative_points", 0) + team_points[away_team_name]

                # Get the standings based on the final results of the current game
                current_standings = get_current_standings(team_points)
                current_rank = get_rank(current_standings)

                home_current_rank = current_rank[home_team_name]
                home_previous_rank =  previous_rank.get(home_team_name, home_current_rank)
                game_data["home_current_standings"] = home_current_rank
                game_data["home_previous_standings"] = previous_rank.get(home_team_name, home_current_rank)
                game_data["home_standings_change"] = home_previous_rank - home_current_rank

                away_current_rank = current_rank[away_team_name]
                away_previous_rank =  previous_rank.get(away_team_name, away_current_rank)                
                game_data["away_current_standings"] = current_rank[away_team_name]
                game_data["away_previous_standings"] = previous_rank.get(away_team_name, away_current_rank)
                game_data["away_standings_change"] = away_previous_rank - away_current_rank

                # Check if either team had a change in standings
                game_data["standings_change_flag"] = 1 if game_data["home_standings_change"] != 0 or game_data["away_standings_change"] != 0 else 0
                
                # Check if the game resulted in a drop in standings for the losing team
                game_data["overtaken_flag"] = 1 if game_data["home_standings_change"] < 0 or game_data["away_standings_change"] < 0 else 0

                game_results.append(game_data)

        # Games played
        game_count_by_season[season] = game_count

        season_standings = get_current_standings(team_points)
        season_rank = get_rank(season_standings)

        standings[season] = season_standings
        rank[season] = season_rank

season_results = []

# Compare current season standings to previous season
for season, teams in standings.items():
    previous_season_standings = standings.get(season - 1)
    previous_season_rank = rank.get(season - 1)

    if previous_season_standings and previous_season_rank:
        current_season_standings = standings.get(season)
        current_season_rank = rank.get(season)

        for team, points in teams.items():
            result = {}
            result["season"] = season
            result["season_formatted"] = f"{season}-{str(season + 1)[-2:]}"
            result["team"] = team
            result["points"] = points
            result["previous_season_points"] = previous_season_standings.get(team, 0)
            result["change_in_points"] = points - previous_season_standings.get(team, points)
            place = current_season_rank[team]
            result["place"] = place
            previous_season_place = previous_season_rank.get(team, 0)
            result["previous_season_place"] = previous_season_place
            result["change_in_place"] = 0 if previous_season_place == 0 else (place - previous_season_place) * -1

            season_results.append(result)

with open(f'./season_results.csv', 'w') as f:
    column_headings = season_results[0].keys()
    f.write(",".join(column_headings) + "\n")
    for row in season_results:
        values = [str(x) for x in row.values()] 
        f.write(",".join(values) + "\n")

with open(f'./game_results.csv', 'w') as f:
    column_headings = game_results[0].keys()
    f.write(",".join(column_headings) + "\n")
    for row in game_results:
        values = [str(x) for x in row.values()] 
        f.write(",".join(values) + "\n")

print(f"\nTotal games processed:\n" + str(game_count_by_season))
