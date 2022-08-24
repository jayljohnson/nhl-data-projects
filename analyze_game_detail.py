from collections import defaultdict
import pickle
from os import listdir
import json

game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'feed/live'

seasons = range(2000, 2022)
regular_season_game_type = '02'
max_game_id = 2100



files = sorted(listdir('data/game-detail/'), reverse=True)
# print(files)
# print(len(files))

skip_files = ["2021-feed-live-2021021307.pkl"]

sea_car_empty_net = "2021-feed-live-2021020892.pkl"

with open(f"data/game-detail/{sea_car_empty_net}", 'rb') as f:
    game = pickle.load(f)

    goalies = {
        "home": game["liveData"]["boxscore"]["teams"]["home"]["goalies"],
        "away": game["liveData"]["boxscore"]["teams"]["away"]["goalies"]
    }

    print(goalies)

    players = game["liveData"]["boxscore"]["teams"]["home"]["players"]

    goalie = players[f"ID{goalies['home'][0]}"]
    print(goalie)

    print()
    # print(game["liveData"].keys())
    

# event_type_ids = defaultdict(lambda: 0)
# counter = 0
# for file in files:
#     if file in skip_files:
#         pass
#     try:
#         with open(f"data/game-detail/{file}", 'rb') as f:
#             game = pickle.load(f)
#             # print(game["gameData"]["datetime"]["dateTime"])
#             # data = json.dumps(game, indent=4)
#             game_datetime = game["gameData"]["datetime"]["dateTime"]
#             # print(game_datetime)
#             if "2022-03-07" in game_datetime:
#                 print("\n")
#                 print(file)
#                 print(game_datetime)
#                 print(game["gameData"]["teams"]["home"]["name"])
#                 print(game["gameData"]["teams"]["away"]["name"])
#             elif "2021" in game_datetime:
#                 break
#             else:
#                 pass
#             # if 'EMERGENCY_GOALTENDER' in data.upper():
#             #     print(file)
#             #     with open(f"data/game-detail-output/test.json", 'w') as o:
#             #         o.write(json.dumps(game, indent=4))
#             #     break
#             # plays = game["liveData"]["plays"]["allPlays"]
#             # for play in plays:
#             #     for play in plays:
#             #         event_type_id = play["result"]["eventTypeId"]
#             #         event_type_ids[event_type_id] += 1
#     except EOFError as e:
#         print(f"Error with file {file},  {e}")
    
#     if file == "2021-feed-live-2021020892.pkl":
#         with open(f"data/game-detail-output/test.json", 'w') as o:
#             o.write(json.dumps(game, indent=4))


