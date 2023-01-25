import requests
import pickle
import json
from os.path import exists
from ..utils import utils

game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'feed/live'

# seasons = range(2000, 2022)
seasons = range(1916, 2023)
regular_season_game_type = '02'
max_game_id = 2200


for season in seasons:
    skip_count = 0
    max_skip_count = 5
    for id in range(1, max_game_id):
        game_id = f"{season}{regular_season_game_type}{str(id).zfill(4)}"
        endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
        filename = f'data/raw/feed-live/{season}-{game_endpoint_subtype.replace("/", "-")}-{game_id}.pkl'

        if exists(filename):
            print(f"File {filename} exists")
            with open(filename, 'rb') as f:
                print(f"Checking game state for filename {filename}")

                file_data = pickle.load(f)
                # print(json.dumps(list(file_data["gameData"]["status"]["abstractGameState"].keys()), indent=4))
                # print(json.dumps(list(file_data.keys()), indent=4))
                game_state = file_data["gameData"]["status"]["abstractGameState"]
                print(f"Game state is: {game_state}")
                if game_state != "Final":
                    print(f"Checking if state changed, at {endpoint}")

                    response, status_code = utils.call_endpoint(endpoint)
                    game_state = game_data["gameData"]["status"]["abstractGameState"]
                    # detect the last game for a season
                    if game_data.get("messageNumber") == 2:
                        skip_count += 1
                        print(f"Game not found, {endpoint}")
                        if skip_count >= max_skip_count:
                            print("Aborting; no more data after 3 retries")
                            break
                    elif status_code != 200:
                        raise Exception(f"Invalid http response, {status_code}")
                    elif game_state != "Final":
                        skip_count += 1
                    else:
                        print("Overwriting with new data")
                        game_data = requests.get(url=endpoint).json()   
                        with open(filename, 'wb') as f:
                            pickle.dump(game_data, f, protocol=4)
                else:
                    skip_count += 1
                    continue
