import json
from datetime import datetime
from os.path import exists
from os import makedirs
from ..utils import utils
from pysqlite3 import dbapi2 as sqlite3

"""
TODO list
---------
# Backfill - fetch data from the api source and if a database already exists replace all of the data
1. Backup existing table (low priority - max number of backups to keep or time-based expiration?)
1. Drop table if exists
1. Create new table
1. Run the incremental load starting from 1917

# Incremental
1. Get the active season.  Poll for next season too.
1. Get the games for the active season
1. Start transaction
1. Remove the "latest record" flag from existing records for a season
1. insert the new record with the "latest record" flag set to True
1. End transaction
1. Create indices if they don't exist

# Product ideas
1. NHL calendar synced to google calendar.  Subscribe to the calendar, with automatic updates and alerts for changes
"""

# WARNING - setting init_db to false wipes out everything
init_db = False
# If true writes to database, else skips writes
load_db = True
TABLE_NAME = "games"
STARTING_SEASON = 2022


# def get_lastet_active_season(connection):
#     """
#
#     @return:
#     """
#
#     # TODO: Change this to get the lesser of the current active season and the future season.
#     #  In-betwen seasons, pull the season that will be starting next
#     #  After stanley cup, stop polling the completed season
#     first_season = 1917
#     db_init(connection=connection, table_name=TABLE_NAME)
#
#     cur = connection.cursor()
#     cur.execute(f"SELECT max(season) as season FROM {TABLE_NAME}")
#
#     print(cur.fetchone())
#     if cur.fetchone():
#         return int(cur.fetchone()[0])
#     else:
#         return first_season


def get_season_games(season_from: int = STARTING_SEASON, season_to: int = None):
    connection = get_db_connection()
    db_init(connection=connection, table_name=TABLE_NAME)

    season_to = season_to or utils.get_current_year() + 1

    for season in range(season_from, season_to):
        # TODO: Need to poll the db to get the statuses.
        #  If any game has non-`Final` status, the latest api data should be inserted into the db
        cached_season_data = read_season_from_db(connection=connection, table_name="games", season=season)
        print(f"Cached season data: {cached_season_data}")
        if cached_season_data:
            remaining_games = {g["status"]['detailedState'] for g in cached_season_data['dates'][-1]['games']}
            print(f"Remaining games: {remaining_games}")
            if "Final" not in remaining_games:
                print("Refreshing the cache with the latest api results")
                refresh_db_season_from_api(connection=connection, season=season)
        else:
            refresh_db_season_from_api(connection=connection, season=season)


def refresh_db_season_from_api(connection, season):
    season_year_end = season + 1
    print(f"Getting game info for season `{season}-{season_year_end}`")
    endpoint = f"https://statsapi.web.nhl.com/api/v1/schedule?season={season}{season_year_end}"
    response, status_code = utils.call_endpoint(endpoint)
    if status_code == 200 and response["totalItems"]:
        print(f"Data found for season: {season}")
        write_to_db(connection=connection, identifier=season, table_name=TABLE_NAME, response=response)
    elif status_code == 200 and not response["totalItems"]:
        print(f"No games published yet for season {season}")
    else:
        raise Exception(f"Unhandled exception with api status {status_code}\n {json.dumps(response, indent=4)}")
    print("Done fetching from api\n")


def get_game_ids_with_state(season):
    connection = get_db_connection()

    cur = connection.cursor()
    cur.execute(f"SELECT details FROM {TABLE_NAME} WHERE season = ? order by as_of_timestamp desc limit 1", [season])

    season_games = json.loads(cur.fetchone()[0])
    print(json.dumps(season_games, indent=4))

    result = []
    for dates in season_games["dates"]:
        for games in dates["games"]:
            game_id = games["gamePk"]
            abstract_game_state = games["status"]["abstractGameState"]
            result.append({game_id: abstract_game_state})
    return result


def get_db_connection():
    sqlite_file_path = f"{utils.DATA_FILE_PATH_RAW}/games/"
    sqlite_file_name = "games.db"
    if not exists(sqlite_file_path):
        makedirs(sqlite_file_path)
    print(f"Opening sqlite db file at {sqlite_file_path}")
    print(f"Establishing sqlite connection")
    return sqlite3.connect(sqlite_file_path + sqlite_file_name)


def db_init(connection, table_name):
    cur = connection.cursor()
    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cur.fetchone() or init_db:
        print(f"Dropping and recreating the databse table {table_name}")
        cur.execute(f"DROP TABLE if exists {table_name}")
        cur.execute(f"""
            CREATE TABLE if not exists {table_name} (
                season, 
                as_of_timestamp, 
                details,
                UNIQUE(season, as_of_timestamp) ON CONFLICT REPLACE
            )
        """)


def write_to_db(connection, table_name, identifier, response):
    if load_db:
        print("Writing to database")
        cur = connection.cursor()
        data = [
            (identifier, datetime.now(), json.dumps(response))
        ]
        cur.executemany(
            f"INSERT INTO {table_name} VALUES(?, ?, ?)", data,
        )
        connection.commit()


def read_season_from_db(connection, table_name, season):
    if load_db:
        print(f"Reading from database for season {season}")
        cur = connection.cursor()
        cur.execute(f"SELECT details FROM {table_name} WHERE season = ?", [season])

        # print(cur.fetchone())
        if cur.fetchone():
            print("Database record found")
            return json.loads(cur.fetchone())
        else:
            print("No database record found")
            return None


def read_season_counts_from_db(connection, table_name, season):
    if load_db:
        print(f"Reading from database for season {season}")
        cur = connection.cursor()
        cur.execute(
            f"""
            SELECT season, count(as_of_timestamp) as as_of_timestamp_count 
            FROM {table_name} 
            GROUP By 1,2
            order by 1 desc
            """
        )

        print(cur.fetchone())
        if cur.fetchone():
            print(f"Summary of database records: {json.loads(cur.fetch())}")
            return json.loads(cur.fetch())
        else:
            print("No database records found")
            return None


get_season_games()
