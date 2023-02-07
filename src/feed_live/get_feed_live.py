import requests
import pickle
import json
from os.path import exists
from ..utils import utils
from .feed_live import RAW_FILE_PATH


print("Getting next batch of game data")

game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'feed/live'


def is_cached_game_final(filename):
    if exists(filename):
        print(f"File {filename} exists")
        with open(filename, 'rb') as f:
            file_data = pickle.load(f)
            previous_game_state = file_data["gameData"]["status"]["abstractGameState"]
            print(f"Game state from last run: {previous_game_state}")
            if previous_game_state == "Final":
                return True
            else:
                return False
    else:
        print(f"Cached game not found at {filename}")
        return False

# TODO: Add support to restart from an earlier season.  Could be simple like deleting or archiving old files.
def incremental():
    """
    Check for existing files (contains cached api responses, unmodified)
    If file exists, get the status from file
    Then get the status from api
    If api status != file status, overwrite the file
    """
    last_season = int(utils.get_filename_list(RAW_FILE_PATH)[-1][0:4])
    seasons = range(last_season, last_season + 2)
    print(f"seasons: {seasons}")

    regular_season_game_type = '02'

    # TODO: if more than 32 teams and/or more than 84 games each make sure this value is big enough
    max_game_id = 2200
    
    # TODO: This is hacky; need handling for cancelled games like Buffalo NY blizzards
    max_skip_count = 10 

    # TODO: Before starting on next dataset, generalize the logic below and make it reusable
    for season in seasons:
        print(f"Getting data for season {season}")
        skip_count = 0

        for id in range(1, max_game_id):
            game_id = f"{season}{regular_season_game_type}{str(id).zfill(4)}"
            endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
            filename = f'data/raw/feed-live/{season}-{game_endpoint_subtype.replace("/", "-")}-{game_id}.pkl'
            print(f"Looking for game data for game_id: {game_id}")
            if not is_cached_game_final(filename):
                # print(f"File {filename} exists")
                # with open(filename, 'rb') as f:
                #     file_data = pickle.load(f)
                #     previous_game_state = file_data["gameData"]["status"]["abstractGameState"]
                #     print(f"Game state from last run: {previous_game_state}")
                #     if previous_game_state != "Final":

                response, status_code = utils.call_endpoint(endpoint)
                # current_game_state = response["gameData"]["status"]["abstractGameState"]
                # print(f"Game state from current run: {current_game_state}")

                if response.get("messageNumber") == 2:
                    skip_count += 1
                    print(f"Game not found, {endpoint}.  Skipped {skip_count} files")
                    if skip_count >= max_skip_count:
                        print(f"Aborting; no more data after {max_skip_count} retries for season {season}")
                        break
                elif status_code != 200:
                    raise Exception(f"Invalid http response, {status_code}")
                else:
                    with open(filename, 'wb') as f:
                        print(f"Writing new data to file {filename}")
                        pickle.dump(response, f, protocol=4)

incremental()

# def backfill():
#     ## Backfill
#     # seasons = range(2000, 2022)
#     seasons = range(1916, 2023)
#     regular_season_game_type = '02'
#     max_game_id = 2200
#     for season in seasons:
#         skip_count = 0
#         max_skip_count = 5
#         for id in range(1, max_game_id):
#             game_id = f"{season}{regular_season_game_type}{str(id).zfill(4)}"
#             endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
#             filename = f'data/raw/feed-live/{season}-{game_endpoint_subtype.replace("/", "-")}-{game_id}.pkl'

#             if exists(filename):
#                 print(f"File {filename} exists")
#                 with open(filename, 'rb') as f:
#                     print(f"Checking game state for filename {filename}")

#                     file_data = pickle.load(f)
#                     # print(json.dumps(list(file_data["gameData"]["status"]["abstractGameState"].keys()), indent=4))
#                     # print(json.dumps(list(file_data.keys()), indent=4))
#                     game_state = file_data["gameData"]["status"]["abstractGameState"]
#                     print(f"Game state is: {game_state}")
#                     if game_state != "Final":
#                         print(f"Checking if state changed, at {endpoint}")

#                         response, status_code = utils.call_endpoint(endpoint)
#                         game_state = game_data["gameData"]["status"]["abstractGameState"]
#                         # detect the last game for a season
#                         if game_data.get("messageNumber") == 2:
#                             skip_count += 1
#                             print(f"Game not found, {endpoint}")
#                             if skip_count >= max_skip_count:
#                                 print("Aborting; no more data after 3 retries")
#                                 break
#                         elif status_code != 200:
#                             raise Exception(f"Invalid http response, {status_code}")
#                         elif game_state != "Final":
#                             skip_count += 1
#                         else:
#                             print("Overwriting with new data")
#                             game_data = requests.get(url=endpoint).json()   
#                             with open(filename, 'wb') as f:
#                                 pickle.dump(game_data, f, protocol=4)
#                     else:
#                         skip_count += 1
#                         continue

