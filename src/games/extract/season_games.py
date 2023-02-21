from os.path import exists
import typing
import logging

from flatten_json import flatten

from src.utils import utils

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

DATASET_NAME = "season_games"
# WARNING - Setting invalidate_cache ignores the database cache and refreshes all data from the api
#           For all games this takes a couple minutes and is okay.  Be careful with larger datasets.
all_expire_immediately = False
scan_keys = True

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
        if all_expire_immediately or self.is_current_season:
            response, status_code, _ = utils.call_endpoint(endpoint, expire_immediately=True)
        else:
            response, status_code, _ = utils.call_endpoint(endpoint)
        logging.debug(f"get games api status_code for season {self.season} is: {status_code}")
        self.data = response
        return self.data

    def get_game_ids_with_state(self):
        result = {}
        for dates in self.data["dates"]:
            for game in dates["games"]:
                game_id = game["gamePk"]
                abstract_game_state = game["status"]["abstractGameState"]
                result[game_id] = (abstract_game_state, self.season)
        self.map_game_id__state = result
        logging.info(f"Number of games for season = {len(result)}")
        return self.map_game_id__state

    def get_game_data(self):
        for dates in self.data["dates"]:
            for game in dates["games"]:
                game["season"] = self.season
                yield game


def insert_season_game(game_data: typing.Dict[str, typing.Any], cursor):

    row_flattened = flatten(game_data)
    logging.info(f"game_id: {row_flattened['gamePk']}, number of records in game_data: {len(game_data)}")
    insert_colname_strings = ", ".join([f"{x}" for x in row_flattened.keys()])
    insert_placeholder_strings = ", ".join([f":{x}" for x in row_flattened.keys()])
    logging.debug(f"row_flattened length: {len(row_flattened)}")
    query = f"""
        INSERT OR REPLACE INTO {DATASET_NAME}
        ({insert_colname_strings})
        VALUES({insert_placeholder_strings})
    """
    cursor.execute(query, row_flattened)


def get_all_game_keys():
    all_game_keys = set()
    file_path_all_keys = f"{utils.DATA_FILE_PATH_OUTPUT}/{DATASET_NAME}_all_keys.csv"

    connection = utils.get_db_connection()
    cursor = connection.cursor()

    if scan_keys or not exists(file_path_all_keys):
        for season in range(SEASON_FROM, SEASON_TO + 1):
            cursor.execute("BEGIN")
            season_games = SeasonGames(season)
            season_games.get()
            for game in season_games.get_game_data():
                all_game_keys.update(flatten(game))
                # TODO: Decouple the scanning from the inserts and trigger from the main feed_live_games calls
                insert_season_game(game_data=game, cursor=cursor)
            connection.commit()
        with open(file_path_all_keys, 'w') as f:
            f.write("\n".join(all_game_keys))
            logging.info(f"at get_all_keys read: {all_game_keys}")
    else:
        with open(file_path_all_keys, 'r') as f:
            all_game_keys = f.read().split("\n")
    logging.info(f"All game keys: {all_game_keys}")
    return sorted(all_game_keys)


def get_latest_season():
    endpoint = "https://statsapi.web.nhl.com/api/v1/seasons/current"
    response, status_code, _ = utils.call_endpoint(endpoint=endpoint, ttl_seconds=60*60*24)
    latest_season = response.get('seasons')[0]
    latest_season_id = latest_season.get('seasonId')
    logging.debug(f"Latest seasonId: {latest_season_id}")
    return int(latest_season_id[0:4])


if __name__ == "__main__":
    all_games = get_all_game_keys()
    # TODO: This needs to be created once the first time, or if the schema changes in the future
    utils.create_table(dataset_name=DATASET_NAME, all_keys=all_games, primary_keys=["gamePk"])
