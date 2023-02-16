import pickle
from os.path import exists

import typing
import json
from flatten_json import flatten

from src.utils import utils
from .season_games import SeasonGames, get_latest_season
from ..constants import RAW_FILE_PATH, OUTPUT_FILE_PATH, DATASET_NAME
import logging

logging.basicConfig(level='DEBUG')

FULL_REFRESH = False


class GameFeedLive:
    def __init__(self, game_id):
        self.game_id = game_id
        self.abstract_game_state = None
        self.data = None

    def get(self):
        game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
        game_endpoint_subtype = 'feed/live'
        endpoint = f"{game_endpoint}/{self.game_id}/{game_endpoint_subtype}"

        print(f"Calling endpoint: {endpoint}")
        self.data, status = utils.call_endpoint(endpoint)
        self.abstract_game_state = self.data["gameData"]["status"]["abstractGameState"]
        print(f"Game status: {self.abstract_game_state}")

        # Games that are not final can change so those are pulled again from the API instead of the cache
        # After a game reaches 'final' status it is always pulled from the cache and does not need to be invalidated
        if self.abstract_game_state != 'Final':
            self.data, status = utils.call_endpoint(endpoint, invalidate_cache=True)
        return self.data


def get_all_game_feed_lives(season_from=None, season_to=None):
    """
    @param season_from:
    @param season_to:
    @return:
    """
    season_from = season_from or get_latest_season()
    season_to = season_to or get_latest_season()
    seasons = range(season_from, season_to + 1)
    print(f"seasons: {seasons}")

    for season in seasons:
        print(f"Getting data for season {season}")

        season_games_obj = SeasonGames(season)
        season_games = season_games_obj.get()
        if FULL_REFRESH:
            game_ids_to_refresh = season_games
        else:
            game_ids_to_refresh = {
                g: a for g, a in season_games_obj.get_game_ids_with_state().items() if a != 'Preview'
            }

        for game_id, abstract_game_state in game_ids_to_refresh.items():
            game_feed_live_obj = GameFeedLive(game_id=game_id)
            game_feed_live_obj.get()
            print(f"Game id is {game_id}; abstract_game_state is {game_feed_live_obj.abstract_game_state}")

            all_keys = get_all_plays_keys(game_feed_live_obj.data)
            write_to_db(season=season, game_id=game_id, all_keys=all_keys, game_feed_live=game_feed_live_obj.data)

            if game_feed_live_obj.abstract_game_state != abstract_game_state:
                raise RuntimeError("State error")


def get_all_plays_keys(game_feed_live):
    # Used to prepare the data for loading into the database
    # This is a brute-force approach, and a new column added to a later season would need a backfill. Not solving for
    # that, yet.
    file_path_all_keys = f"{OUTPUT_FILE_PATH}/{DATASET_NAME}-all-keys.pkl"
    if exists(file_path_all_keys):
        with open(file_path_all_keys, 'rb+') as f:
            all_keys = set(pickle.load(f))
    else:
        all_keys = set()

    feed_live_game_live = game_feed_live["liveData"]["plays"]["allPlays"]

    for row in feed_live_game_live:
        row_flat = flatten(row)
        row_keys = list(row_flat.keys())
        all_keys.update(row_keys)

    all_keys_sorted = sorted(all_keys)

    with open(file_path_all_keys, 'wb') as f:
        # print(f"at all_keys_sorted: {all_keys_sorted}")
        pickle.dump(all_keys_sorted, f, protocol=4)
    return all_keys_sorted


def write_to_db(season, game_id, all_keys, game_feed_live: typing.Dict[str, typing.Any]):

    default_columns = {
        "season": season,
        "game_id": game_id
    }

    sqlite_file_path = f"{RAW_FILE_PATH}-db/game_feed_live.db"
    connection = utils.get_db_connection(sqlite_file_path)

    all_plays = game_feed_live["liveData"]["plays"]["allPlays"]
    # print(f"all_plays type: {all_plays}")
    if all_plays:
        cur = connection.cursor()
        all_keys_with_default_columns = list(default_columns.keys()) + list(all_keys)

        all_plays_flattened_list = []
        for row in all_plays:
            # print(type(row))
            row.update(default_columns)
            row_flattened = flatten(row)
            if len(row_flattened["coordinates"]) == 0:
                row_flattened["coordinates"] = None
            result_dict = {k: row_flattened.get(k, None) for k in all_keys_with_default_columns}
            # print(f"result_dict length: {len(result_dict)}")
            all_plays_flattened_list.append(result_dict)
        print(f"Number of records: {len(all_plays_flattened_list)}")
        # print(json.dumps(all_plays_flattened_list, indent=4))
        # all_plays_flattened.update(default_columns)

        # flattened_output = flatten(result_dict)
        # print(f"Number of records: {flattened_output}")
        # if len(flattened_output["coordinates"]) == 0:
        #     flattened_output["coordinates"] = None

        # print(flattened_output)

        create_table_columns = ",\n".join([x.replace('\'', '') for x in all_keys_with_default_columns])
        create_table_sql = f"create table if not exists feed_live_games({create_table_columns})"
        # print(f"create table sql: {create_table_sql}")
        cur.execute(create_table_sql)

        # TODO: This is not safe from injection attacks - simplify

        insert_placeholder_strings = ", ".join([f":{x}" for x in all_keys_with_default_columns])
        query = f"INSERT INTO feed_live_games VALUES({insert_placeholder_strings})"
        # print(query)

        # query = "insert into feed_live_games " + \
        #         str(tuple(flattened_output.keys())).replace('\'', '') + " values" + \
        #         str(tuple(flattened_output.values())).replace('None', 'NULL') + ";"
        cur.executemany(query, all_plays_flattened_list)
        # cur.executemany(
        #     "INSERT INTO feed_live_games VALUES(%s)", flattened_output.values()
        # )
        connection.commit()
        cur.close()
    else:
        cur = connection.cursor()
        print("No data; inserting placeholder")
        sql = "insert into feed_live_games (season, game_id) VALUES (?, ?)"
        cur.execute(sql, (season, game_id))
        connection.commit()
        cur.close()
        print(f"all_keys: {all_keys}")
        print("all_plays: " + str(game_feed_live["liveData"]["plays"].keys()))
        # raise Exception("Unhandled insert")

    # print("**** NO DATA ****")


if __name__ == "__main__":
    get_all_game_feed_lives(season_from=1917)





# def db_init(connection):
#     if init_db:
#         print("Dropping and recreating database table")
#         cur = connection.cursor()
#         cur.execute("DROP TABLE if exists game_feed_live")
#         cur.execute("""
#             CREATE TABLE if not exists game_feed_live(
#                 game_id PRIMARY KEY,
#                 as_of_timestamp,
#                 details,
#                 UNIQUE(game_id, as_of_timestamp) ON CONFLICT REPLACE
#                 )
#             """
#         )
#         cur.execute("VACUUM")


# def db_create_views_and_indices(connection):
#     print("Setting up db views and indices")
#     cursor = connection.cursor()
#
#     cursor.execute("drop view if exists game_feed_live_v;")
#     cursor.execute("""
#     create view game_feed_live_v as
#     select
#         game_id,
#         details ->> '$.link' as endpoint,
#         details ->> '$.gameData.status.abstractGameState' as abstract_game_state
#     from game_feed_live;
#     """)
#     cursor.execute("CREATE INDEX if not exists game_feed_live__game_id on game_feed_live(game_id)")
#     cursor.execute("""
#     CREATE INDEX if not exists game_feed_live__abstract_game_state
#     ON game_feed_live(details ->> '$.gameData.status.abstractGameState');
#     """)


# def db_drop_indices(connection):
#     print("Dropping indices")
#     cur = connection.cursor()
#     cur.execute("DROP INDEX if exists game_feed_live__game_id;")
#     cur.execute("DROP INDEX if exists game_feed_live__abstract_game_state;")
#
#


"""
sqlite3 data/raw/feed-live-db/game_feed_live.db ".read src/feed_live/sql/feed_live_v_select.sql"
"""
