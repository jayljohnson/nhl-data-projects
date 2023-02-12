import pickle
import json
from pysqlite3 import dbapi2 as sqlite3

from os.path import exists
from ..utils import utils
from ..games import get_games
from .feed_live import RAW_FILE_PATH

# TODO: Move to .feed_live constants, and also rename the file to something more descriptive
game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'feed/live'

# TODO: Still some edge cases, need to review files for completeness of game_id ranges, data.
#  Probably easier to do this after loading to sqlite
# Configs to control backfill
full_backfill = False
init_db = True
load_db = True
starting_season = 1917

print(sqlite3.sqlite_version)


def check_cached_game_status(filename, abstract_game_status="Final"):
    if exists(filename):
        print(f"File {filename} exists")
        with open(filename, 'rb') as f:
            file_data = pickle.load(f)
            previous_game_state = file_data["gameData"]["status"]["abstractGameState"]
            print(f"Game state from last run: {previous_game_state}")
            if previous_game_state == abstract_game_status:
                print(f"Cached game state {previous_game_state} matches {abstract_game_status}:\n\t {filename}")
                return True, file_data
            else:
                print(f"Cached game state {previous_game_state} does not match {abstract_game_status}:\n\t {filename}")
                return False, file_data
    else:
        print(f"Cached game not found at {filename}")
        return False, None


def incremental(full_backfill: bool = False, starting_season=None):
    """
    Check for existing files (contains cached api responses, unmodified)
    If file exists, get the status from file
    Then get the status from api
    If api status != file status, overwrite the file
    @param full_backfill: Overwrite any existing file with the api response.
                          If no files exist, ignores the game state and pulls all previews or even other game states
    @param starting_season:
    @return:
    """
    connection = get_db_connection()
    db_init(connection)
    print("Done init db")

    if not starting_season:
        starting_season = int(utils.get_filename_list(RAW_FILE_PATH)[-1][0:4])

    current_year = utils.get_current_year()
    seasons = range(starting_season, current_year + 1)
    print(f"seasons: {seasons}")

    max_bad_responses = 3
    max_previews = 10

    for season in seasons:
        print(f"Getting data for season {season}")

        bad_response_count = 0
        consecutive_preview_count = 0

        for game_id, abstract_game_state in get_games.get_game_ids_with_state(season=season).items():
            print(f"Game id is {game_id}")

            endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
            print(f"Using endpoint: {endpoint}")
            filename = f'data/raw/feed-live/{season}-{game_endpoint_subtype.replace("/", "-")}-{game_id}.pkl'
            print(f"Using filename: {filename}")

            cached_game_status, details = check_cached_game_status(filename=filename, abstract_game_status="Final")
            if not cached_game_status:
                response, status_code = utils.call_endpoint(endpoint)
                write_to_db(connection, game_id, response)

                if response.get("messageNumber") == 2:
                    bad_response_count += 1
                    print(f"Game not found, {endpoint}.  Skipped {bad_response_count} files")
                    if bad_response_count >= max_bad_responses:
                        print(
                            f"Aborting; no more data after {max_bad_responses} retries for season {season}"
                        )
                        break
                elif status_code != 200:
                    raise Exception(f"Invalid http response, {status_code}")
                else:
                    current_game_state = response["gameData"]["status"]["abstractGameState"]
                    if full_backfill:
                        # Ignore game state and overwrite any existing file with the api response.
                        pass
                    elif current_game_state == "Preview":
                        consecutive_preview_count += 1
                        print(json.dumps(response, indent=4))
                        if consecutive_preview_count >= max_previews:
                            print(
                                f"Aborting; found {consecutive_preview_count} consecutive previews for season {season}"
                            )
                            break
                        continue
                    elif current_game_state != "Final":
                        bad_response_count += 1
                        print(f"Unhandled game state {current_game_state}, {endpoint}")
                        if bad_response_count >= max_bad_responses:
                            print(
                                f"Aborting; {bad_response_count} bad responses for season {season}"
                            )
                            break
                    # TODO: Write to sqlite too.
                    #  full api endpoint as indexed column name, and dump the raw json to a text column.
                    #  use a view to get the game_id or other attributes as needed
                    with open(filename, 'wb') as f:
                        print(f"Writing new data to file {filename}")
                        pickle.dump(response, f, protocol=4)
                        # Reset the consecutive_preview_count after successful write
                        # to only count consecutive previews
                        consecutive_preview_count = 0
            else:
                write_to_db(connection, game_id, details)
    db_create_views_and_indices(connection)


def get_db_connection():
    sqlite_file_path = f"{RAW_FILE_PATH}-db/game_feed_live.db"
    print(f"Opening sqlite db file at {sqlite_file_path}")
    return sqlite3.connect(sqlite_file_path)


def db_init(connection):
    if init_db:
        print("Dropping and recreating database table")
        cur = connection.cursor()
        cur.execute("DROP TABLE if exists game_feed_live")
        cur.execute("CREATE TABLE if not exists game_feed_live(game_id PRIMARY KEY, details)")


def db_create_views_and_indices(connection):
    print("Setting up db views and indices")
    cursor = connection.cursor()

    cursor.execute("drop view if exists game_feed_live_v;")
    cursor.execute("""
    create view game_feed_live_v as
    select
        game_id,
        details ->> '$.link' as endpoint,
        details ->> '$.gameData.status.abstractGameState' as abstract_game_state
    from game_feed_live;
    """)
    cursor.execute("CREATE INDEX if not exists game_feed_live__game_id on game_feed_live(game_id)")
    cursor.execute("""
    CREATE INDEX if not exists game_feed_live__abstract_game_state 
    ON game_feed_live(details ->> '$.gameData.status.abstractGameState');
    """)


def db_drop_indices(connection):
    print("Dropping indices")
    cur = connection.cursor()
    cur.execute("DROP INDEX if exists game_feed_live__game_id;")
    cur.execute("DROP INDEX if exists game_feed_live__abstract_game_state;")


def write_to_db(connection, game_id, response):
    if load_db:
        cur = connection.cursor()
        data = [
            (game_id, json.dumps(response))
        ]
        cur.executemany(
            "INSERT INTO game_feed_live VALUES(?, ?)", data
        )
        connection.commit()


incremental(
    full_backfill=full_backfill,
    starting_season=starting_season
)

"""
sqlite3 data/raw/feed-live-db/game_feed_live.db ".read src/feed_live/sql/feed_live_v_select.sql"
"""
