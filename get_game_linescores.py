import requests
import pickle

game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'linescore'

seasons = range(2000, 2022)
regular_season_game_type = '02'
max_game_id = 2100

for season in seasons:
    season_data = []
    for id in range(1, max_game_id):
        # detect the last game for a season
        if game_data.get("messageNumber") == 2:
            break
        else:
            game_id = f"{season}{regular_season_game_type}{str(id).zfill(4)}"
            endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
            print(endpoint)
            game_data = requests.get(url=endpoint).json()
            print(game_data)
            season_data.append(game_data)

    with open(f'data/linescore/{season}-{game_endpoint_subtype}.pkl', 'wb') as f:
        pickle.dump(season_data, f, pickle.HIGHEST_PROTOCOL)
