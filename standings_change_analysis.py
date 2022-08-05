from collections import defaultdict
import pickle

seasons = range(2000, 2022)
standings = {}
rank = {}
game_results = []
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

# TODO: The conference is mapped to team_id. Need to do ranking with team_id instead of team_name 
def get_conference_rank(current_standings):
    result = {}
    previous = None
    for conference in ['Eastern', 'Western']:
        conference_standings = {team_id: points for team_id, points in current_standings.items() if team_id_info[team_id]["conference"]["name"] == conference}
        conference_rank = {}
        for i, (team, position) in enumerate(conference_standings.items()):
            if team_id_info[team]["conference"]["name"] == conference:
                if position != previous:
                    rank, previous = i + 1, position
                conference_rank[team] = rank
            result[conference] = conference_rank
    return result

def get_standings_rank_impact_games_percent(game_results, game_count_by_season, level="league"):
    if level == "league":
        rank_field = "standings_rank_impact_game"
    elif level == 'conference':
        rank_field = "conference_standings_rank_impact_game"
    else:
        raise Exception(f"Invalid level: {level}")

    standings_rank_impact_games = defaultdict(lambda: 0)
    for game in game_results:
        standings_rank_impact_games[game["season"]] += game[rank_field]
    print(f"{level}-level standings change count:\n {dict(standings_rank_impact_games)}\n")

    standings_rank_impact_games_percent = defaultdict(lambda: 0)
    for season, count in game_count_by_season.items():
        standings_rank_impact_games_percent[season] = round(standings_rank_impact_games[season] / count * 100, 3) if count != 0 else None

    print(f"{level}-level standings change percent:\n {dict(standings_rank_impact_games_percent)}\n")

def get_team_info():
    with open(f'./teams_single.pkl', 'rb') as f:
        teams = pickle.load(f)
        # Atlanta Thrashers don't have the conference name from the teams API (team id=11)
        teams[11]["conference"]["name"] = "Eastern"
        print("\n".join([f"{v['name']}, id: {v['id']}, conference: {v['conference'].get('name', 'n/a')}" for k, v in teams.items()]))
        return teams

team_id_info = get_team_info()

def get_game_stats(team_id_info, game_data, teams, home_or_away):
    team = teams.get(home_or_away)

    team_name = team["team"]["name"]
    team_id = team["team"]["id"]
    game_data[f"{home_or_away}_team_id"] = team_id
    game_data[f"{home_or_away}_team"] = team_name
    conference = team_id_info[game_data[f"{home_or_away}_team_id"]]["conference"].get("name")
    game_data[f"{home_or_away}_conference"] = conference
    team_goals = team["goals"]
    game_data[f"{home_or_away}_goals"] = team_goals
    game_data[f"{home_or_away}_shots_on_goal"] = team["shotsOnGoal"]
    return team_id,conference,team_goals

for season in seasons:
    game_count = 0
    team_points = defaultdict(lambda: 0)

    with open(f'./{season}-linescore.pkl', 'rb') as f:
        games = pickle.load(f)
        for i, game in enumerate(games):
            if game.get("messageNumber") != 2:
                previous_standings_points = get_current_standings(team_points)
                previous_standings_ranks = get_rank(previous_standings_points)
                previous_conference_ranks = get_conference_rank(previous_standings_points)

                # team.goaliePulled is always False and team.numSkaters doesn't make sense.  why?
                # It would be interesting to see stats on pulled goalie conversion to goals scored.
                game_count += 1
                game_data = {}
                game_data["season"] = season
                game_id = f"{season}02{str(i).zfill(4)}"
                game_data["game_id"] = game_id

                teams = game.get("teams")

                home_team_id, home_conference, home_team_goals = get_game_stats(team_id_info, game_data, teams, home_or_away="home")

                away_team_id, away_conference, away_team_goals = get_game_stats(team_id_info, game_data, teams, home_or_away="away")

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
                team_points[home_team_id] += game_data["home_points"]
                game_data["home_team_cumulative_points"] = game_data.get("home_team_cumulative_points", 0) + team_points[home_team_id]
                team_points[away_team_id] += game_data["away_points"]
                game_data["away_team_cumulative_points"] = game_data.get("away_team_cumulative_points", 0) + team_points[away_team_id]

                # Get the standings based on the final results of the current game
                current_standings_points = get_current_standings(team_points)
                current_rank = get_rank(current_standings_points)
                current_conference_rank = get_conference_rank(current_standings=current_standings_points)

                home_current_rank = current_rank[home_team_id]
                home_previous_standings_ranks =  previous_standings_ranks.get(home_team_id, home_current_rank)

                game_data["**home league ranks**"] = "---> home league rank"
                game_data["home_current_standings_rank"] = home_current_rank
                game_data["home_previous_standings_rank"] = previous_standings_ranks.get(home_team_id, home_current_rank)
                game_data["home_standings_rank_change"] = home_previous_standings_ranks - home_current_rank

                away_current_rank = current_rank[away_team_id]
                away_previous_standings_ranks =  previous_standings_ranks.get(away_team_id, away_current_rank)

                game_data["**away league ranks**"] = "---> away league rank"
                game_data["away_current_standings_rank"] = current_rank[away_team_id]
                game_data["away_previous_standings_rank"] = previous_standings_ranks.get(away_team_id, away_current_rank)
                game_data["away_standings_rank_change"] = away_previous_standings_ranks - away_current_rank

                # Check if either team had a change in standings.  
                # To show the frequency of standings changes throughout the season
                game_data["**standings change**"] = "---> standings change"
                game_data["standings_change_flag"] = 1 if game_data["home_standings_rank_change"] != 0 or game_data["away_standings_rank_change"] != 0 else 0
                
                # Check if the game resulted in the winning team overtaking the losing team in the standings
                # To show the number of high impact games throughout the season
                if game_data["home_current_standings_rank"] > game_data["away_previous_standings_rank"] and game_data["home_previous_standings_rank"] <= game_data["away_previous_standings_rank"]:
                    game_data["home_overtook_flag"], game_data["away_overtook_flag"] = (1,0)
                elif game_data["away_current_standings_rank"] > game_data["home_previous_standings_rank"]  and game_data["away_previous_standings_rank"] <= game_data["home_previous_standings_rank"]:
                    game_data["home_overtook_flag"], game_data["away_overtook_flag"] = (0,1)
                else:
                    game_data["home_overtook_flag"], game_data["away_overtook_flag"] = (0,0)
                game_data["standings_rank_impact_game"] = 1 if 1 in [game_data["home_overtook_flag"], game_data["away_overtook_flag"]] else 0

                ### Conference standings
                home_current_conference_rank = current_conference_rank[home_conference][home_team_id]
                if previous_conference_ranks.get(home_conference):
                    home_previous_conference_rank = previous_conference_ranks[home_conference].get(home_team_id, 1)
                else:
                    home_previous_conference_rank = home_current_conference_rank
                game_data["**home conference_ranks**"] = "---> home conference rank"
                game_data["home_current_conference_rank"] = home_current_conference_rank
                game_data["home_previous_conference_rank"] = home_previous_conference_rank
                game_data["home_conference_rank_change"] = home_previous_conference_rank - home_current_conference_rank                

                away_current_conference_rank = current_conference_rank[away_conference][away_team_id]
                if previous_conference_ranks.get(away_conference):
                    away_previous_conference_rank = previous_conference_ranks[away_conference].get(away_team_id, 1)
                else:
                    away_previous_conference_rank = away_current_conference_rank
                game_data["**away conference_ranks**"] = "---> away conference rank"    
                game_data["away_current_conference_rank"] = away_current_conference_rank
                game_data["away_previous_conference_rank"] = away_previous_conference_rank
                game_data["away_conference_rank_change"] = away_previous_conference_rank - away_current_conference_rank

                # Check if either team had a change in standings.  
                # To show the frequency of standings changes throughout the season
                game_data["**conference standings change**"] = "---> conference standings change"
                game_data["conference_standings_change_flag"] = 1 if game_data["home_conference_rank_change"] != 0 or game_data["away_conference_rank_change"] != 0 else 0
                
                # Check if the game resulted in the winning team overtaking the losing team in the standings
                # To show the number of high impact games throughout the season
                if game_data["home_current_conference_rank"] > game_data["away_previous_conference_rank"] and game_data["home_previous_conference_rank"] <= game_data["away_previous_conference_rank"]:
                    game_data["home_conference_overtook_flag"], game_data["away_conference_overtook_flag"] = (1,0)
                elif game_data["away_current_conference_rank"] > game_data["home_previous_conference_rank"]  and game_data["away_previous_conference_rank"] <= game_data["home_previous_conference_rank"]:
                    game_data["home_conference_overtook_flag"], game_data["away_conference_overtook_flag"] = (0,1)
                else:
                    game_data["away_conference_overtook_flag"], game_data["home_conference_overtook_flag"] = (0,0)
                game_data["conference_standings_rank_impact_game"] = 1 if 1 in [game_data["home_conference_overtook_flag"], game_data["away_conference_overtook_flag"]] else 0

                game_results.append(game_data)

        # Games played
        game_count_by_season[season] = game_count

        season_standings_points = get_current_standings(team_points)
        season_standings_rank = get_rank(season_standings_points)

        standings[season] = season_standings_points
        rank[season] = season_standings_rank

season_results = []

# Compare current season standings to previous season
for season, teams in standings.items():
    # The 2004-05 labor dispute resulted in the entire season being cancelled.  
    # In 2005, compare to the results from 2003 instead of 2004
    # TODO: Compare Arizona Coyotes to Phoenix Coyotes (team move/rename from Phoenix)
    # TODO: Compare Winnipeg Jets to Atlanta Thrashers (team move from Atlanta)
    previous_season_standings = standings.get(season - 1) if season != 2005 else standings.get(2003)
    previous_season_rank = rank.get(season - 1) if season != 2005 else standings.get(2003)

    # Skip the season for 2004 because of the lockout.
    if previous_season_standings and previous_season_rank and season != 2004:
        current_season_standings = standings.get(season)
        current_season_rank = rank.get(season)
        current_conference_rank = get_conference_rank(current_standings=current_season_standings)

        for team_id, points in teams.items():
            result = {}
            result["season"] = season
            result["season_formatted"] = f"{season}-{str(season + 1)[-2:]}"
            result["team_id"] = team_id
            result["team_name"] = team_id_info[team_id]["name"]
            result["conference"] = team_id_info[team_id]["conference"]["name"]
            result["points"] = points
            result["previous_season_points"] = previous_season_standings.get(team_id, 0)
            result["change_in_points"] = points - previous_season_standings.get(team_id, points)
            place = current_season_rank[team_id]
            result["place"] = place
            previous_season_place = previous_season_rank.get(team_id, 0)
            result["previous_season_place"] = previous_season_place
            result["change_in_place"] = 0 if previous_season_place == 0 else (place - previous_season_place) * -1
            result["conference_place"] = current_conference_rank[team_id_info[team_id]["conference"]["name"]][team_id]

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

print(f"\nTotal games processed:\n" + str(game_count_by_season) + "\n")

get_standings_rank_impact_games_percent(game_results, game_count_by_season, "league")

# TODO: Separate breakdown of stats by conference?
get_standings_rank_impact_games_percent(game_results, game_count_by_season, "conference")