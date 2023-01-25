import sqlite3
from ..utils import utils
from os.path import exists
from os import remove

player_ids_file = f"{utils.DATA_FILE_PATH_OUTPUT}/players/player_ids.pkl"
if utils.confirm_prompt("Delete player_id's and re-scan the database?"):
    remove(player_ids_file)

if exists(player_ids_file):
    print("Player_ids_file exists")
    player_ids = utils.open_pickle_file(player_ids_file)
else:
    print("Checking the database for player_ids")
    conn = sqlite3.connect(f"{utils.DATA_FILE_PATH_OUTPUT}/sqlite/nhl.db")

    cur = conn.cursor()
    cur.execute("""
    select distinct players_0_player_id from [feed-live]
    union
    select distinct players_1_player_id from [feed-live]
    union
    select distinct players_2_player_id from [feed-live]
    union
    select distinct players_3_player_id from [feed-live]
    """)
    player_ids = cur.fetchall()
    utils.write_pickle_file(f"{utils.DATA_FILE_PATH_OUTPUT}/players/player_ids.pkl", player_ids)

player_ids_count = len(player_ids)
print(f"Player id count: {player_ids_count}")

for i, player_id in enumerate(player_ids):
    player_id_value = player_id[0]
    endpoint = f"https://statsapi.web.nhl.com/api/v1/people/{player_id_value}"
    player_filename = f"{utils.DATA_FILE_PATH_RAW}/players/player_id_{player_id_value}.pkl"
    print(f"{endpoint}, player {i} of {player_ids_count}")

    if not exists(player_filename):
        response, status_code = utils.call_endpoint(endpoint)
        if status_code == 200:
            utils.write_pickle_file(player_filename, response)
        else:
            print(response, status_code)
    else:
        print(f"Skipping {endpoint}, file already exists at {player_filename}")