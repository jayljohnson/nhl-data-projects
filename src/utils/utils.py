import pickle
import requests
import typing
from os import remove, makedirs
from os.path import exists, dirname
from flatten_json import flatten

DATA_FILE_PATH_RAW = "data/raw"
DATA_FILE_PATH_OUTPUT = "data/outputs"

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
    
    # return pd.json_normalize(data)

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


def call_endpoint(endpoint):
    response = requests.get(url=endpoint)
    status_code = response.status_code
    return response.json(), status_code
