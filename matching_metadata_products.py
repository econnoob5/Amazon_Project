import pandas as pd
import json
import os
import numpy as np

# The following two functions are used to parse the NOT GZIPPED json files and to open the as a Pandas DataFrame
def parse(path):
    with open(path) as jsonfile:
        for line in jsonfile:
            yield json.loads(line)


def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')

# set server test directories
# server_load_dir_test = r"/data/users/mlaudi/Amazon/Data/Tests/Metadata_test_load"
# server_save_dir_test = r"/data/users/mlaudi/Amazon/Data/Tests/Metadata_test_save"

# server full run directories
# server_load_dir = r"/data/users/mlaudi/Amazon/Data/Metadata_small"
# server_save_support = r"/data/users/mlaudi/Amazon/Data/support_data"

# set PC test directories
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_load"
SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_save"
metadata_sample_dir = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
save_dir = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

# set firms-products directory
csv_file = os.path.join(PROJECT_ROOT_DIR + "Amazon Project - Data/firms_products_link/firms_products_skimmed.csv")
df_csv_file = pd.read_csv(csv_file)

for file in os.listdir(metadata_sample_dir):
    filename = "/" + file
    df = getDF(metadata_sample_dir + filename)
