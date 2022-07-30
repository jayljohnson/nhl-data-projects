import pickle

seasons = range(2000, 2022)
standings = {}
game_count = 0

for season in seasons:
    game_results = []
    with open(f'./{season}-linescore.pkl', 'rb') as f:
        games = pickle.load(f)
        for i, game in enumerate(games):
            game_count += 1
            if game.get("messageNumber") != 2:
                game_data = {}
                game_data["season"] = season
                game_data["game_id"] = f"{season}02{str(i).zfill(4)}"

                teams = game.get("teams")

                home_team = teams.get("home")
                home_team_name = home_team["team"]["name"]
                game_data["home_team"] = home_team_name
                home_team_goals =  home_team["goals"]
                game_data["home_goals"] = home_team_goals
                game_data["home_shots_on_goal"] = home_team["shotsOnGoal"]

                away_team = teams.get("away")
                away_team_name = away_team["team"]["name"]
                game_data["away_team"] = away_team_name
                away_team_goals = away_team["goals"]
                game_data["away_goals"] = away_team_goals
                game_data["away_shots_on_goal"] = away_team["shotsOnGoal"]

                if home_team_goals > away_team_goals:
                    game_data["win"] = home_team_name
                    game_data["loss"] = away_team_name
                    game_data["win_points"] = 2
                elif home_team_goals < away_team_goals:
                    game_data["win"] = away_team_name
                    game_data["loss"] = home_team_name
                    game_data["win_points"] = 2
                else:
                    game_data["win"] = None
                    game_data["loss"] = None
                    game_data["win_points"] = 0

                final_period = game.get("currentPeriod")
                # * Starting from 2005-06, games tied after overtime go to a shootout
                # * Starting from 1999-00, an overtime loss earns 1 point for the losing team 
                # final period key: regulation = 3, overtime = 4, shootout = 5
                if final_period == 3:
                    game_finish_status = "regulation"
                    game_data["loss_points"] = 0
                elif final_period == 4:
                    game_finish_status = "ot"
                    game_data["loss_points"] = 1
                elif final_period == 5:
                    game_finish_status = "so"
                    game_data["loss_points"] = 1
                else:
                    # for irregular games like covid cancellations
                    game_finish_status = None
                    game_data["win_points"] = 0
                    game_data["loss_points"] = 0

                game_data["game_finish_status"] = game_finish_status

                game_results.append(game_data)

        # Standings
        teams = {}
        for game in game_results:
            if not teams.get(game["win"]) and game["win"]:
                teams[game["win"]] = 0
            if not teams.get(game["loss"]) and game["loss"]:
                teams[game["loss"]] = 0

            if game.get("win") and game.get("loss"):
                teams[game["win"]] += game["win_points"]
                teams[game["loss"]] += game["loss_points"]

        season_standings = {k: v for k, v in sorted(teams.items(), key=lambda item: item[1], reverse=True)}
        standings[season] = season_standings

final_results = []

for season, teams in standings.items():
    previous_season = standings.get(season - 1)
    if previous_season:
        previous_season_places = {team: place for place, team in enumerate(previous_season.keys(), 1)}

    for place, (team, points) in enumerate(teams.items(), 1):
        result = {}
        if previous_season:
            result["season"] = season
            result["season_formatted"] = f"{season}-{str(season + 1)[-2:]}"
            result["team"] = team
            result["points"] = points
            result["previous_season_points"] = previous_season.get(team, 0)
            result["change_in_points"] = points - previous_season.get(team, points)
            result["place"] = place
            previous_season_place = previous_season_places.get(team, 0)
            result["previous_season_place"] = previous_season_place
            result["change_in_place"] = 0 if previous_season_place == 0 else (place - previous_season_place) * -1

            final_results.append(result)

with open(f'./final_result.csv', 'w') as f:
    f.write(",".join(final_results[0].keys()) + "\n")
    for row in final_results:
        values = [str(x) for x in row.values()] 
        f.write(",".join(values) + "\n")

print(f"\nTotal games processed: {game_count}")
