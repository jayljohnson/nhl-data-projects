import pickle
from os.path import exists
import json

import typing
from flatten_json import flatten

from src.utils import utils
from .season_games import SeasonGames, get_latest_season
from ..constants import RAW_FILE_PATH, OUTPUT_FILE_PATH, DATASET_NAME
import logging

FULL_REFRESH = False
SQLITE_FILE_PATH = f"{RAW_FILE_PATH}-db/game_feed_live.db"
CALL_COUNT = 0


class GameFeedLive:
    # TODO: Pass in abstractGameState from the SeasonGames responses;
    #  if state == 'Final' write to cache else expire immediately
    def __init__(self, game_id, abstract_game_state_current = None):
        self.game_id = game_id
        self.abstract_game_state = abstract_game_state_current
        self.data = None
        self.is_cached = None
        self.endpoint = None

    def get(self):
        game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
        game_endpoint_subtype = 'feed/live'
        self.endpoint = f"{game_endpoint}/{self.game_id}/{game_endpoint_subtype}"

        logging.debug(f"Calling endpoint: {self.endpoint}")
        # Games that are not final can change so those are pulled from the API without caching
        # After a game reaches 'Final' status it is cached forever
        if self.abstract_game_state == 'Final':
            self.data, status, self.is_cached = utils.call_endpoint(self.endpoint)
        else:
            logging.info(f"Invalidating cache for game_id {self.game_id}, game_state {self.abstract_game_state}")
            self.data, _, self.is_cached = utils.call_endpoint(self.endpoint, invalidate_cache=True)
        self.abstract_game_state = self.data["gameData"]["status"]["abstractGameState"]
        logging.debug(f"Game status: {self.abstract_game_state}")

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
    logging.debug(f"seasons: {seasons}")

    for season in seasons:
        game_ids_with_season = get_game_ids_to_refresh(season)
        logging.debug(f"at game_ids_to_refresh: {list(game_ids_with_season)}")

        for game_info in game_ids_with_season:
            game_data = get_data_for_single_game_id(game_info)
            logging.debug(f"at game_data: {game_data}")
            logging.debug(list(game_data))
        logging.info(f"Done with season {season}")


def get_game_ids_to_refresh(season):
    logging.debug(f"Getting data for season {season}")

    season_games_obj = SeasonGames(season)
    season_games_obj.get()
    game_ids_to_refresh = season_games_obj.get_game_ids_with_state()

    if not FULL_REFRESH:
        game_ids_to_refresh = [
            (g, v[0], v[1]) for g, v in game_ids_to_refresh.items() if v[0] != 'Preview'
        ]

    return game_ids_to_refresh


def get_data_for_single_game_id(game_ids_with_season):
    game_id, abstract_game_state, season = game_ids_with_season
    game_feed_live_obj = GameFeedLive(game_id=game_id, abstract_game_state_current=abstract_game_state)
    game_feed_live_obj.get()
    logging.debug(f"Game id is {game_id}; game_feed_live_obj.abstract_game_state is {game_feed_live_obj.abstract_game_state}")
    logging.debug(f"Input abstract_game_state is: {abstract_game_state}")

    all_keys = get_all_plays_keys(game_feed_live_obj.data)
    default_columns = get_default_columns(game_id, season)
    logging.info(f"Game is cached: {game_feed_live_obj.is_cached}")

    global CALL_COUNT
    if CALL_COUNT == 0:
        create_table_feed_live_games(all_keys=all_keys, default_columns=default_columns)
        CALL_COUNT += 1

    if FULL_REFRESH or not game_feed_live_obj.is_cached:
        # Games that had a cache miss don't exist in the db either; write them to the db
        # Do the same for when FULL_REFRESH is True
        logging.info(f"Writing {game_id} to database.")
        write_to_db(season=season, game_id=game_id, all_keys=all_keys, game_feed_live=game_feed_live_obj.data)
    else:
        # Skip inserts for rows that were already cached.  These should have already been parsed and inserted.
        logging.info(f"Game {game_id} is already in the db; skipping write")
    return game_feed_live_obj.data


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
        pickle.dump(all_keys_sorted, f, protocol=4)
    return all_keys_sorted


def write_to_db(season, game_id, all_keys, game_feed_live: typing.Dict[str, typing.Any]):

    default_columns = get_default_columns(game_id, season)

    connection = utils.get_db_connection(SQLITE_FILE_PATH)
    cur = connection.cursor()
    cur.execute("BEGIN")
    all_plays = game_feed_live["liveData"]["plays"]["allPlays"]
    logging.info(f"game_id: {game_id}, number of records in all_plays: {len(all_plays)}")
    if all_plays:
        for row in all_plays:
            row.update(default_columns)
            row_flattened = flatten(row)
            if not row_flattened.get("coordinates") or row_flattened.get("coordinates") == '{}':
                row_flattened["coordinates"] = None
            logging.debug(f"Number of records: {len(row_flattened)}")
            logging.debug(f"all_plays_flattened_list: {json.dumps(row_flattened, indent=4)}")

            insert_colname_strings = ", ".join([f"{x}" for x in row_flattened.keys()])
            insert_placeholder_strings = ", ".join([f":{x}" for x in row_flattened.keys()])
            logging.debug(f"row_flattened length: {len(row_flattened)}")
            query = f"""
                INSERT OR REPLACE INTO feed_live_games
                ({insert_colname_strings})
                VALUES({insert_placeholder_strings})
            """
            cur.execute(query, row_flattened)
    else:
        logging.debug("No data; inserting placeholder")
        sql = "insert into feed_live_games (season, game_id) VALUES (?, ?)"
        cur.execute(sql, (season, game_id))
        logging.debug(f"all_keys: {all_keys}")
        logging.debug("all_plays: " + str(game_feed_live["liveData"]["plays"].keys()))
        # raise Exception("Unhandled insert")
    connection.commit()
    cur.close()


def get_default_columns(game_id, season):
    default_columns = {
        "season": season,
        "game_id": game_id
    }
    return default_columns


def create_table_feed_live_games(all_keys, default_columns):
    connection = utils.get_db_connection(SQLITE_FILE_PATH)
    all_keys_with_default_columns = list(default_columns.keys()) + list(all_keys)

    column_constraints = {
        "season": "NOT NULL",
        "game_id": "NOT NULL"
    }

    columns_with_constraints = [x + " " + column_constraints.get(x, "") for x in all_keys_with_default_columns]

    create_table_columns = ",\n".join([x.replace('\'', '') for x in columns_with_constraints])
    create_table_sql = f"""
        create table if not exists feed_live_games({create_table_columns},
                                                   PRIMARY KEY(season, game_id, about_eventId)
    )
    """
    logging.info(f"Creating table: {create_table_sql}")

    cur = connection.cursor()
    cur.execute(create_table_sql)


if __name__ == "__main__":
    get_all_game_feed_lives(season_from=1917, season_to=2022)

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
