from collections import defaultdict
import pickle
from os import listdir
import json
import csv
from . import shift_charts
from ..utils import utils


import os
# cwd = os.getcwd()
# print(f"current working directory: {cwd}")

# dir_path = os.path.dirname(os.path.realpath(__file__))
# print(f"directory path: {dir_path}")

from pathlib import Path 

files = sorted(listdir("./" + shift_charts.RAW_FILE_PATH), reverse=True)

# sea_car_empty_net = "2021-2021020892.pkl"
# game_id = "2022020515"  # STL-SEA Dec2022

first_row = True
with open(shift_charts.OUTPUT_FILENAME, 'a') as f:
    csv_writer = csv.writer(f)

    for file in files:
        print(f"Converting file to csv: {file}")
        data = utils.open_pickle_file(f"{shift_charts.RAW_FILE_PATH}/{file}")["data"]
        for row in data:
            if first_row:
                header = row.keys()
                csv_writer.writerow(header)
                first_row = False
            csv_writer.writerow(row.values())
