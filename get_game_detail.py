import requests
import pickle
from os.path import exists

game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'feed/live'

# seasons = range(2000, 2022)
seasons = range(1916, 2023)
regular_season_game_type = '02'
max_game_id = 2100

for season in seasons:
    skip_count = 0
    max_skip_count = 2
    for id in range(1, max_game_id):
        game_id = f"{season}{regular_season_game_type}{str(id).zfill(4)}"
        endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
        filename = f'data/game-detail/{season}-{game_endpoint_subtype.replace("/", "-")}-{game_id}.pkl'

        if exists(filename):
            print(f"File {filename} already exists; skipping")
        else:
            response = requests.get(url=endpoint)
            game_data = response.json()

            # detect the last game for a season
            if game_data.get("messageNumber") == 2:
                skip_count += 1
                print(f"Game not found, {endpoint}")
                if skip_count >= max_skip_count:
                    print(game_data)
                    break
            elif response.status_code != 200:
                raise Exception(f"Invalid http response, {response.status_code}")
            else:
                print(endpoint)
                game_data = requests.get(url=endpoint).json()
                with open(filename, 'wb') as f:
                    pickle.dump(game_data, f, pickle.HIGHEST_PROTOCOL)
