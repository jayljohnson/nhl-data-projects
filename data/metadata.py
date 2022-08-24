# from collections import defaultdict
# import pickle
# from os import listdir
# import json

# game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
# game_endpoint_subtype = 'feed/live'

# seasons = range(2000, 2022)
# regular_season_game_type = '02'
# max_game_id = 2100



# files = sorted(listdir('data/game-detail/'))
# # print(files)
# # print(len(files))

# with open(f"data/game-detail/{files[-1]}", 'rb') as f:
#     game = pickle.load(f)

#     with open(f"data/game-detail-output/test.json", 'w') as o:
#         o.write(json.dumps(game, indent=4))

# event_type_ids = defaultdict(lambda: 0)
# for file in files:
#     try:
#         with open(f"data/game-detail/{file}", 'rb') as f:
#             game = pickle.load(f)
#             plays = game["liveData"]["plays"]["allPlays"]
#             for play in plays:
#                 for play in plays:
#                     event_type_id = play["result"]["eventTypeId"]
#                     event_type_ids[event_type_id] += 1
#     except EOFError as e:
#         print(f"Error with file {file},  {e}")

# print(json.dumps(event_type_ids, indent=4))

EVENT_TYPE_IDS = {
    "PENALTY": 44706537,
    "GOAL": 30591891,
    "MISSED_SHOT": 104888550,
    "SHOT": 249118638,
    "GAME_SCHEDULED": 4421903,
    "PERIOD_READY": 14855879,
    "PERIOD_START": 14873682,
    "FACEOFF": 262746222,
    "HIT": 204687054,
    "GIVEAWAY": 78817553,
    "BLOCKED_SHOT": 125921574,
    "STOP": 204880308,
    "TAKEAWAY": 61971754,
    "PERIOD_END": 14852947,
    "PERIOD_OFFICIAL": 14852567,
    "GAME_END": 4423197,
    "SHOOTOUT_COMPLETE": 510564,
    "GAME_OFFICIAL": 1033841,
    "EARLY_INT_START": 10563,
    "EARLY_INT_END": 10563,
    "EMERGENCY_GOALTENDER": 3369,
    "CHALLENGE": 409443
}