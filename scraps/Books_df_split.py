import pandas as pd
import os
import json

""""""""""""""""""""""""""""""""""""""""""""""" Create functions """""""""""""""""""""""""""""""""""""""""""""""""""""
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


""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# ----------------- test directories -----------------
# set PC test directories
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Metadata_small-brand_nodup/meta_Books.json"
SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/metadata_test_save"
dir_load = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

# ----------------- full run directories on IESE server -----------------
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_small-brand_nodup/meta_Books.json"
# dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Books Chunks for DataFrame"

df_book = getDF(dir_load)

n = 350000
list_df = [df_book[i:i+n] for i in range(0, df_book.shape[0], n)]
size = df_book.shape
print(df_book.shape)