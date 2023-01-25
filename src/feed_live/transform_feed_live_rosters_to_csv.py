import json
from os import listdir
import pandas as pd
from ..utils import utils
from ..feed_live import feed_live as fl

# TODO: This should run along with the main feed-live transforms since it is already loading the pickle files from disk
#  Loading the files again has an extra I/O cycle

files = sorted(listdir(fl.RAW_FILE_PATH), reverse=True)
OUTPUT_FILE_GAME_SUMMARY = f"{fl.OUTPUT_FILE_PATH}/game-summary-feed-live.csv"
OUTPUT_FILE_GAME_ROSTER = f"{fl.OUTPUT_FILE_PATH}/game-roster-feed-live.csv"

data_files = [OUTPUT_FILE_GAME_SUMMARY, OUTPUT_FILE_GAME_ROSTER]
if utils.confirm_prompt(
    "Delete existing csv's before running?  "
    "By default, data is appended the files so it is a good idea to delete"
    ):
    utils.remove_files(data_files)
    utils.make_data_file_paths(data_files)

is_first_file = True
file_count = len(files)
with open(OUTPUT_FILE_GAME_SUMMARY, 'a') as f_game_summary:
    with open(OUTPUT_FILE_GAME_ROSTER, 'a') as f_game_roster:

        for i, file in enumerate(files):
            print(f"At file {i} of {file_count}")
            data = utils.open_pickle_file(f"{fl.RAW_FILE_PATH}/{file}")
            data_short = data["liveData"]["boxscore"]
            data_short_keys = list(data_short.keys())
            game_id = data["gamePk"]
            game_data = data["gameData"]
            game_data["gameId"] = game_id
            game_state = game_data["status"]["abstractGameState"]
            if not game_state == "Final":
                print(f"Skipping file {file} because game_state is: `{game_state}`")
                continue

            f_game_summary.write(pd.json_normalize(game_data).to_csv(index=False, header=is_first_file))

            players_reformatted = list(dict())
            for opponent_type in ["home", "away"]:
                team_id = data_short["teams"][opponent_type]["team"]["id"]
                # player_data = list(data_short["teams"][opponent_type]["players"].items())
                player_data_dict = data_short["teams"][opponent_type]["players"]

                for player_id, data in player_data_dict.items():
                    players_reformatted.append({
                        "gameId": game_id,
                        "teamId": team_id,
                        "playerId": player_id,
                        "home_or_away": opponent_type,
                        "player_data": data
                    })

                data_short["teams"][opponent_type]["players"] = players_reformatted

            f_game_roster.write(pd.json_normalize(players_reformatted).to_csv(index=False, header=is_first_file))

            is_first_file = False
