import asyncio
import typing
import datetime
from os import remove, makedirs, listdir
from os.path import exists, dirname
import pickle

from requests_cache import CachedSession
from flatten_json import flatten
# from pysqlite2 import dbapi2 as sqlite3
import sqlite3
import logging

logging.basicConfig(level='INFO')

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
    session = CachedSession(
        'data/raw/http_cache',
        backend='sqlite',
        serializer='json',
        decode_content=True
    )
    return session


def call_endpoint(endpoint, invalidate_cache=False, ttl_seconds=None):
    # TODO: Should be able to write raw json when this is implemented using the decode_content option
    #  https://github.com/requests-cache/requests-cache/issues/631
    session = get_session()
    if invalidate_cache and not ttl_seconds:
        logging.info(f"Setting cache to expire immediately for endpoint: {endpoint}")
        response = session.get(
            url=endpoint,
            headers={'Cache-Control': 'no-cache'},
            expire_after=0
        )
    elif invalidate_cache and ttl_seconds:
        raise RuntimeError(
            f"invalidate_cache or ttl_seconds are incompatible with each other.  "
            f"Pick one or the other and retry."
        )
    elif ttl_seconds:
        logging.info(f"Setting cache ttl to {ttl_seconds} seconds for endpoint {endpoint}")
        response = session.get(
            url=endpoint,
            expire_after=ttl_seconds
        )
    else:
        logging.info(f"Setting cache to never expire for endpoint {endpoint}")
        response = session.get(
            url=endpoint,
            expire_after=-1
        )
    status_code = response.status_code
    logging.debug(f"response from_cache: {response.from_cache}, status_code = {status_code}")
    return response.json(), status_code, response.from_cache


def get_db_connection(db_file_path):
    logging.debug(f"Opening sqlite db file at {db_file_path}")
    conn = sqlite3.connect(db_file_path)
    conn.execute('pragma journal_mode=wal')
    return conn
