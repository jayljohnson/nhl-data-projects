import asyncio
import typing
import datetime
from os import remove, makedirs, listdir
from os.path import exists, dirname, getsize
import pickle

from requests_cache import CachedSession
from flatten_json import flatten
# from pysqlite2 import dbapi2 as sqlite3
import sqlite3
import logging

logname = "/var/log/nhl-data-projects"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(logname),
        logging.StreamHandler()
    ]
)


DATA_FILE_PATH_RAW = "data/raw"
DATA_FILE_PATH_OUTPUT = "data/outputs"


def get_current_year():
    return datetime.date.today().year


def open_pickle_file(file_path):
    with open(file_path, 'rb') as f:
        result = pickle.load(f)
        return result


def write_pickle_file(file_path, data):
    file_path_dir = dirname(file_path)
    if not exists(file_path_dir):
        makedirs(file_path_dir)
    with open(file_path, 'wb') as f:
        pickle.dump(data, f, protocol=4)


def json_normalize_new(data):
    return flatten(data)


def json_to_csv(data):
    return data


def write_object_to_csv(file_path, data, print_header: bool, f = None):
    file_path_dir = dirname(file_path)
    if not exists(file_path_dir):
        makedirs(file_path_dir)
    # TODO: How to keep the context manager alive and reuse it when csv's are written inside of a loop?
    if f:
        # f.write(pd.json_normalize(data).to_csv(index=False, header=print_header))
        f.write(json_normalize_new(data).to_csv(index=False, header=print_header))
    else:
        with open(file_path, 'a') as f:
            # f.write(pd.json_normalize(data).to_csv(index=False, header=print_header))
            f.write(json_normalize_new(data).to_csv(index=False, header=print_header))
            return f


def confirm_prompt(question: str) -> bool:
    reply = None
    while reply not in ("y", "n"):
        reply = input(f"{question} (y/n): ").lower()
    return (reply == "y")


def make_data_file_paths(file_paths: typing.List[str]):
    for path in file_paths:
        directory = dirname(path)
        if not exists(directory):
            makedirs(directory)


def remove_files(file_paths: typing.List[str]):
    for path in file_paths:
        if exists(path):
            remove(path)


def get_filename_list(filepath):
    result = sorted(listdir(filepath))
    return result


def get_session():
    sqlite_db_path = 'data/raw/http_cache'
    session = CachedSession(
        sqlite_db_path,
        backend='sqlite',
        serializer='json',
        decode_content=True
    )
    return session


def call_endpoint(endpoint, expire_immediately=False, ttl_seconds=None):
    # TODO: Should be able to write raw json when this is implemented using the decode_content option
    #  https://github.com/requests-cache/requests-cache/issues/631
    session = get_session()
    if expire_immediately and ttl_seconds:
        raise RuntimeError(
            f"invalidate_cache or ttl_seconds are incompatible with each other.  "
            f"Pick one or the other and retry."
        )
    elif expire_immediately and not ttl_seconds:
        logging.debug(f"Setting cache to expire immediately for endpoint: {endpoint}")
        response = session.get(
            url=endpoint,
            headers={'Cache-Control': 'no-cache'},
            expire_after=0
        )
    elif ttl_seconds:
        logging.debug(f"Setting cache ttl to {ttl_seconds} seconds for endpoint {endpoint}")
        response = session.get(
            url=endpoint,
            expire_after=ttl_seconds
        )
    else:
        logging.debug(f"Setting cache to never expire for endpoint {endpoint}")
        response = session.get(
            url=endpoint,
            expire_after=-1
        )
    status_code = response.status_code
    logging.info(f"response from_cache: {response.from_cache}, status_code: {status_code},\n\t endpoint: {endpoint}")
    return response.json(), status_code, response.from_cache


def get_db_connection(db_file_path=f"{DATA_FILE_PATH_OUTPUT}/nhl-data.db"):
    logging.debug(f"Opening sqlite db file at {db_file_path}")
    conn = sqlite3.connect(db_file_path)
    conn.execute('pragma journal_mode=wal')
    return conn


# def get_all_keys(dataset_name):
#     file_path_all_keys = f"{DATA_FILE_PATH_OUTPUT}/{dataset_name}_all_keys.pkl"
#     # if not exists(file_path_all_keys):
#     #     logging.info(f"Creating empty file at {file_path_all_keys}")
#     #     with open(file_path_all_keys, 'wb') as f:
#     #         default = set()
#     #         pickle.dump(default, f, protocol=4)
#
#     if exists(file_path_all_keys):
#         logging.info(f"File exists at {file_path_all_keys}")
#         with open(file_path_all_keys, 'rb') as f:
#             existing_keys = set(pickle.load(f))
#             logging.info(f"at get_all_keys read: {existing_keys}")
#             return existing_keys
#     else:
#         return set()
#         # logging.info(f"File not found at {file_path_all_keys}; creating empty file")
#         # raise RuntimeError(f"Unhandled at {file_path_all_keys}")
#         # with open(file_path_all_keys, 'wb') as f:
#         #     default = set()
#         #     pickle.dump(default, f, protocol=4)
#         #     return default
#
#
# def merge_all_keys(dataset_name, new_data):
#     logging.debug(f"At merge_all_keys new_data: {new_data}")
#
#     file_path_all_keys = f"{DATA_FILE_PATH_OUTPUT}/{dataset_name}_all_keys.pkl"
#     existing_keys = get_all_keys(dataset_name=dataset_name)
#
#     with open(file_path_all_keys, 'rb+') as f:
#         new_data_flat = flatten(new_data)
#         new_keys = set(new_data_flat.keys())
#         logging.debug(f"at get_all_keys before write: {new_keys}")
#
#         existing_keys.update(new_keys)
#         # f.truncate()
#         pickle.dump(existing_keys, f, protocol=4)
#     return existing_keys


def create_table(dataset_name, all_keys, primary_keys, default_columns=None, column_constraints=None):
    connection = get_db_connection()
    logging.info(f"all keys: {all_keys}")

    # default_columns = default_columns or ["season", "game_id"]

    if default_columns:
        all_keys_with_default_columns = list(all_keys) + list(default_columns)
    else:
        all_keys_with_default_columns = list(all_keys)

    column_constraints = column_constraints or {
        "season": "NOT NULL",
        "game_id": "NOT NULL"
    }

    columns_with_constraints = [x + " " + column_constraints.get(x, "") for x in all_keys_with_default_columns]

    create_table_columns = ",\n".join([x.replace('\'', '') for x in columns_with_constraints])
    primary_keys_str = ",\n".join(primary_keys)

    create_table_sql = f"""
        create table if not exists {dataset_name}({create_table_columns},
                                                PRIMARY KEY({primary_keys_str})
    )
    """
    logging.info(f"Creating table: {create_table_sql}")

    cur = connection.cursor()
    cur.execute(create_table_sql)
    connection.close()
