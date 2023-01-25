from collections import defaultdict
from read_feed_live import get_feed_live
from read_shifts import get_shifts
import json

import pandas as pd


game_id = "2021020892"

feed_live = get_feed_live(game_id)
shift_data = get_shifts(game_id)


def get_player_details(feed_live):
    boxscore_teams = feed_live["liveData"]["boxscore"]["teams"]

    players = dict()
    home = boxscore_teams["home"]
    home_team_id = home["team"]["id"]
    away = boxscore_teams["away"]
    away_team_id = away["team"]["id"]
    
    for player_id, info in home["players"].items():
        info["team_id"] = home_team_id
        players[player_id] = info
    
    for player_id, info in away["players"].items():
        info["team_id"] = away_team_id
        players[player_id] = info

    return players

def get_player(players, player_id):
    return players[f"ID{player_id}"]

def get_player_position(players, player_id):
    player = get_player(players, player_id)
    return player["position"]["code"]

def get_goalie_shifts():
    players = get_player_details(feed_live)

    goalie_shifts = []
    for shift in shift_data:
        player_id = shift["playerId"]
        position = get_player_position(players, player_id)

        if position == 'G':
            goalie_shifts.append(shift)

    df = pd.json_normalize(goalie_shifts)
    print(df)
    print(df.groupby(['teamId', 'period'])['id'].count())

    return goalie_shifts

goalie_shifts = get_goalie_shifts()

"""
event_type, 
shift       
play
"""

"""
number of shifts
goalie more than 3 shifts
- if mid-game, likely a delayed penalty
number of goalies with shifts by team
goalie total ice time
goalie changed
goalie pulled while down in the 3rd for extra attacker
goal for while goalie pulled
goal against while goalie pulled
tie after goalie pull
come from behind win after goalie pulled
"""