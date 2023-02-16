import pickle
from os import remove
from os.path import exists
from flatten_json import flatten
from src.utils import utils
from .. import constants as fl
from ..extract.feed_live_games import GameFeedLive

# DATASET_NAME = "feed-live"
# RAW_FILE_PATH = f"{utils.DATA_FILE_PATH_RAW}/{DATASET_NAME}"
# OUTPUT_FILE_PATH = f"{utils.DATA_FILE_PATH_OUTPUT}/{DATASET_NAME}"

# OUTPUT_FILENAME = f"{OUTPUT_FILE_PATH}/{DATASET_NAME}.csv"


# TODO: This data should be queryable from sqlite; try parsing required values using a db view
def get_game_year_from_filename(filename):
    return filename.split("/")[-1][0:4]


def get_game_id_from_filename(filename):
    game_id = filename.split("/")[-1][15:25]
    return game_id


def get_feed_live(game_id):
    return GameFeedLive(game_id=game_id).get()


def get_feed_live_old(filename):
    # year = get_game_year_from_filename(filename)
    # game_id = get_game_id_from_filename(filename)
    # filename = f"{RAW_FILE_PATH}/{year}-{DATASET_NAME}-{game_id}.pkl"
    with open(filename, 'rb') as f:
        result = pickle.load(f)
        return result


def get_distinct_keys():
    """
    Get the unique set of keys across all files.  
    Needed so that the csv header contains the superset of all column names based on the unique set of keys
    Caching to a file to save on compute time for re-runs.  Delete the file to do a full re-run.
    """
    # FILE_PATH_ALL_KEYS = f"{fl.OUTPUT_FILE_PATH}/{fl.DATASET_NAME}-all-keys.pkl"
    if exists(fl.FILE_PATH_ALL_KEYS):
        with open(fl.FILE_PATH_ALL_KEYS, 'rb') as f:
            all_keys = sorted(pickle.load(f))
    else:
        all_keys = set()
        filename_list_feed_live = utils.get_filename_list()
        for i, game_file in enumerate(filename_list_feed_live):
            print(f"at {game_file}, file number: {i}")
            feed_live_data = get_feed_live(f"{fl.RAW_FILE_PATH}/{game_file}")

            feed_live_game_live = feed_live_data["liveData"]["plays"]["allPlays"]

            for row in feed_live_game_live:
                row_flat = flatten(row)
                row_keys = list(row_flat.keys())
                all_keys.update(row_keys)

        all_keys_sorted = sorted(all_keys)

        with open(fl.FILE_PATH_ALL_KEYS, 'wb') as f:
            pickle.dump(all_keys_sorted, f, protocol=4)
    return all_keys


def to_csv(count, game_file, feed_live_game_live):
    output_filename=fl.OUTPUT_FILENAME
    # Get the superset of all attribute names across all files
    all_keys = get_distinct_keys()

    with open(output_filename, 'a') as f:
        # print(f"Destination file: {output_filename}")

        season = get_game_year_from_filename(game_file)
        game_id = get_game_id_from_filename(game_file)

            # extra_attributes = {
            #     "season": season,
            #     "game_id": game_id
            # }

            # extra_attribute_keys = ",".join(extra_attributes.keys())
            # extra_attribute_values = ",".join(extra_attributes.values())

            # Flattening logic for keys that don't exist in a file
        for row in feed_live_game_live:
            row_flat = flatten(row)
            if count == 0:
                header = "season,gameId," + ",".join(all_keys) + "\n"  # TODO: Better handling for extra file-level vars
                f.write(header)
                count += 1
            row_str = f"\"{season}\",\"{game_id}\","  # TODO: Better handling for extra file-level vars
            column_count = 0
            for row_key in all_keys:
                col_str = "\"" + str(row_flat.get(row_key, "")).replace("\"", "'") +"\""  # Cleanup to replace double quotes in the data with single quotes
                if column_count == 0:
                    row_str += col_str
                else:
                    row_str += "," + col_str
                column_count += 1
            f.write(row_str + "\n")
        f.close()


def write_csv():  
    # Delete existing file before loading data
    if exists(fl.OUTPUT_FILENAME):
        if utils.confirm_prompt(
            f"Do you want to replace the existing csv file `{fl.OUTPUT_FILENAME}`?  This may take a while.."):
            remove(fl.OUTPUT_FILENAME)
        else:
            print("Aborting...")
            exit()

    count = 0
    filename_list_feed_live = utils.get_filename_list()
    file_count = len(filename_list_feed_live)
    for i, game_file in enumerate(filename_list_feed_live):
        print(f"Processing {i} of {file_count} files")
        source_path = f"{fl.RAW_FILE_PATH}/{game_file}"
        feed_live = get_feed_live(source_path)
        # print(f"Source file: {source_path}")

        feed_live_game_live = c

        to_csv(count, game_file, feed_live_game_live)
        count += 1

write_csv()
