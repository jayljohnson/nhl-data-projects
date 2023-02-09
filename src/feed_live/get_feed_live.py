import pickle
import json
import datetime
from enum import Enum
from os.path import exists
from ..utils import utils
from .feed_live import RAW_FILE_PATH

# TODO: Move to .feed_live constants, and also rename the file to something more descriptive
game_endpoint = 'https://statsapi.web.nhl.com/api/v1/game'
game_endpoint_subtype = 'feed/live'

# TODO: Still some edge cases, need to review files for completeness of game_id ranges, data.
#  Probably easier to do this after loading to sqlite
# Configs to control backfill
full_backfill = False
starting_season = 2000


def check_cached_game_status(filename, abstract_game_status="Final"):
    if exists(filename):
        print(f"File {filename} exists")
        with open(filename, 'rb') as f:
            file_data = pickle.load(f)
            previous_game_state = file_data["gameData"]["status"]["abstractGameState"]
            print(f"Game state from last run: {previous_game_state}")
            if previous_game_state == abstract_game_status:
                print(f"Cached game state {previous_game_state} matches {abstract_game_status}:\n\t {filename}")
                return True
            else:
                print(f"Cached game state {previous_game_state} does not match {abstract_game_status}:\n\t {filename}")
                return False
    else:
        print(f"Cached game not found at {filename}")
        return False


class GameTypes(Enum):
    PRESEASON = '01'
    REGULAR_SEASON_GAME_TYPE = '02'
    PLAYOFF = '03'
    ALL_STAR = '04'

    def __str__(self):
        return self.value

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def get_game_ids(season, game_type):
    if season >= 2021:
        game_id_count = 1312
    elif 2017 <= season < 2021:
        game_id_count = 1271
    else:
        game_id_count = 1230

    game_ids = []
    if game_type in [str(GameTypes.PRESEASON), str(GameTypes.REGULAR_SEASON_GAME_TYPE)]:
        for i in range(1, game_id_count):
            game_id = f"{season}{game_type}{str(i).zfill(4)}"
            game_ids.append(game_id)
    elif game_type == str(GameTypes.PLAYOFF):
        for playoff_round in range(1, 4):
            playoff_round_matchups = {
                1: 8,
                2: 4,
                3: 2,
                4: 1
            }
            for matchup in range(1, playoff_round_matchups[playoff_round]):
                for game_number in range(1, 7):
                    game_id = f"{season}030{playoff_round}{matchup}{game_number}"
                    game_ids.append(game_id)
    elif game_type == str(GameTypes.ALL_STAR):
        all_star_game_ids = get_allstart_game_ids(season=season)
        game_ids.extend(all_star_game_ids)
    else:
        raise RuntimeError(f"Unhandled game_type: {game_type}")
    return game_ids


def get_allstart_game_ids(season):
    season_year_end = season + 1
    endpoint = f"https://statsapi.web.nhl.com/api/v1/schedule?gameType=A&season={season}{season_year_end}"
    response, status_code = utils.call_endpoint(endpoint)

    games = [date["games"] for date in response["dates"]]
    game_ids = [g[0]["gamePk"] for g in games]
    return game_ids


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
    if not starting_season:
        starting_season = int(utils.get_filename_list(RAW_FILE_PATH)[-1][0:4])

    current_year = datetime.date.today().year
    seasons = range(starting_season, current_year + 1)
    print(f"seasons: {seasons}")

    max_bad_responses = 3
    max_previews = 10

    for season in seasons:
        print(f"Getting data for season {season}")

        for game_type in GameTypes.list():
            bad_response_count = 0
            consecutive_preview_count = 0

            for game_id in get_game_ids(season=season, game_type=game_type):
                print(f"Game id is {game_id}")

                endpoint = f"{game_endpoint}/{game_id}/{game_endpoint_subtype}"
                print(f"Using endpoint: {endpoint}")
                filename = f'data/raw/feed-live/{season}-{game_endpoint_subtype.replace("/", "-")}-{game_id}.pkl'
                print(f"Using filename: {filename}")

                if not check_cached_game_status(filename=filename, abstract_game_status="Final"):
                    response, status_code = utils.call_endpoint(endpoint)
                    if response.get("messageNumber") == 2:
                        bad_response_count += 1
                        print(f"Game not found, {endpoint}.  Skipped {bad_response_count} files")
                        if bad_response_count >= max_bad_responses:
                            print(f"Aborting; no more data after {max_bad_responses} retries for season {season}")
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
                                print(f"Aborting; {bad_response_count} bad responses for season {season}")
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


incremental(full_backfill=full_backfill, starting_season=starting_season)
