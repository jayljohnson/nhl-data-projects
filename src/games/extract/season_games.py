from src.utils import utils
import logging

"""
TODO list
---------
# Backfill - fetch data from the api source and if a database already exists replace all of the data
1. Backup existing table (low priority - max number of backups to keep or time-based expiration?)
1. Drop table if exists
1. Create new table
1. Run the incremental load starting from 1917

# Incremental
1. [done] Get the active season's data
1. Get the mapping of game_id to status for the active season from the cache
1. For each game_id 
    1. Get the feed-live api result for the game_id, with expires set to Never
        1. if status != 'Final':
            1. Get the feed-live api result for the game_id, with expires set to Never
                1. Find out if cache headers filtering is applicable?
            1. If feed-live api status != 'Final', change expires to Immediate.  
                TODO: Is this possible??
        1. elif status == 'Final:
            1.  this isn't needed, api call was already set to expiries Never
1. Create indices if they don't exist

# Product ideas
1. NHL calendar synced to google calendar.  Subscribe to the calendar, with automatic updates and alerts for changes
2. Update calendar entry with live odds, news links aggregated X hours before with an alarm, links to favorite websites.  
        UI is the calendar entry.
"""

# WARNING - Setting invalidate_cache ignores the database cache and refreshes all data from the api
#           For all games this takes a couple minutes and is okay.  Be careful with larger datasets.
invalidate_cache = False

# PARAMS
SEASON_FROM = 1917
SEASON_TO = 2022


class SeasonGames:
    STARTING_SEASON = 1917

    def __init__(self, season):
        self.season = season
        self.data = None
        self.map_game_id__state = None
        self.is_current_season = season == get_latest_season()
        if season < self.STARTING_SEASON:
            raise ValueError(f"Invalid SEASON_FROM; no data exists before season {self.STARTING_SEASON}")

    def get(self):
        season_year_end = self.season + 1
        logging.debug(f"Getting game info for season `{self.season}-{season_year_end}`")
        endpoint = f"https://statsapi.web.nhl.com/api/v1/schedule?season={self.season}{season_year_end}"
        if invalidate_cache or self.is_current_season:
            response, status_code, _ = utils.call_endpoint(endpoint, invalidate_cache=True)
        else:
            response, status_code, _ = utils.call_endpoint(endpoint)
        logging.debug(f"get games api status_code for season {self.season} is: {status_code}")
        self.data = response
        return self.data

    def get_game_ids_with_state(self):
        result = {}
        for dates in self.data["dates"]:
            for games in dates["games"]:
                game_id = games["gamePk"]
                abstract_game_state = games["status"]["abstractGameState"]
                result[game_id] = abstract_game_state
        self.map_game_id__state = result
        logging.debug(f"Number of games for season = {len(result)}")
        return self.map_game_id__state


def get_all_games():
    for season in range(SEASON_FROM, SEASON_TO + 1):
        season_games = SeasonGames(season)
        season_games.get()
        season_games.get_game_ids_with_state()


def get_latest_season():
    endpoint = "https://statsapi.web.nhl.com/api/v1/seasons/current"
    response, status_code, _ = utils.call_endpoint(endpoint=endpoint, ttl_seconds=60*60*24)
    latest_season = response.get('seasons')[0]
    latest_season_id = latest_season.get('seasonId')
    logging.debug(f"Latest seasonId: {latest_season_id}")
    return int(latest_season_id[0:4])


if __name__ == "__main__":
    get_all_games()


# def get_db_connection():
#     sqlite_file_path = f"{utils.DATA_FILE_PATH_RAW}/games/"
#     sqlite_file_name = "games.db"
#     if not exists(sqlite_file_path):
#         makedirs(sqlite_file_path)
#     print(f"Opening sqlite db file at {sqlite_file_path}")
#     print(f"Establishing sqlite connection")
#     return sqlite3.connect(sqlite_file_path + sqlite_file_name)


# def db_init(connection, table_name):
#     cur = connection.cursor()
#     cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
#     if not cur.fetchone() or init_db:
#         print(f"Dropping and recreating the databse table {table_name}")
#         cur.execute(f"DROP TABLE if exists {table_name}")
#         cur.execute(f"""
#             CREATE TABLE if not exists {table_name} (
#                 season,
#                 as_of_timestamp,
#                 details,
#                 UNIQUE(season, as_of_timestamp) ON CONFLICT REPLACE
#             )
#         """)
#

# def write_to_db(connection, table_name, identifier, response):
#     if load_db:
#         print("Writing to database")
#         cur = connection.cursor()
#         data = [
#             (identifier, datetime.now(), json.dumps(response))
#         ]
#         cur.executemany(
#             f"INSERT INTO {table_name} VALUES(?, ?, ?)", data,
#         )
#         connection.commit()


# def read_season_from_db(connection, table_name, season):
#     print(f"Reading from database for season {season}")
#     cur = connection.cursor()
#     cur.execute(
#         f"SELECT details FROM {table_name} WHERE season = ? order by as_of_timestamp desc limit 1", [season]
#     )
#
#     results = cur.fetchone()
#     if results:
#         # print(results)
#         print("Database record found")
#         return json.loads(results[0])
#     else:
#         print("No database record found")
#         return None


# def read_season_counts_from_db(connection, table_name):
#     cur = connection.cursor()
#     cur.execute(
#         f"""
#         SELECT
#             season,
#             count(as_of_timestamp) as as_of_timestamp_count
#         FROM {table_name}
#         GROUP BY 1
#         ORDER BY 1 desc
#         """
#     )
#
#     # print(cur.fetchone())
#     result = cur.fetchall()
#     if result:
#         print(f"Summary of database records: {result}")
#         return result
#     else:
#         print("No database records found")
#         return None