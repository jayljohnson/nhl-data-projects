import requests
import pickle
from time import sleep
from os.path import exists
from . import shift_charts


game_endpoint = 'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId='
seasons = range(2010,2011)
regular_season_game_type = '02'
max_game_id = 2100

for season in seasons:
    season_data = []
    for id in range(1, max_game_id):
        game_id = f"{season}{regular_season_game_type}{str(id).zfill(4)}"
        endpoint = f"{game_endpoint}={game_id}"
        game_data = requests.get(url=endpoint).json()
        filename = f"data/raw/{shift_charts.DATASET_NAME}/{season}-{shift_charts.DATASET_NAME}-{game_id}.pkl"
        print(endpoint)
        if exists(filename):
            print(f"File {filename} already exists; skipping")
        else:
            # detect the last game for a season
            if not game_data.get("data"):
                print(game_data, len(game_data))
                # break
            else:
                print("Writing data to file")

                with open(filename, 'wb') as f:
                    pickle.dump(game_data, f, protocol=4)
