"""""""""
Code PURPOSE: dropping unnecessary items from 'Reviews' files from the Amazon datasets to make them lighter and easier
              to handle

Code OUTPUT:

Code MECHANICS:
"""""""""

import pandas as pd
import gzip
import json
import os


# The following two functions are used to parse the GZIPPED json files and to open the as a Pandas DataFrame
def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)


def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


# The following function is use to save a json file to a directory
def writeToJSONFile(save_path, fileName, data):
    filePathNameWExt = save_path + '/' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp, indent=2)


# server directory
dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Reviews"
dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Reviews_small"
dir_save_support = r"/data/users/mlaudi/Amazon/Amazon Project - Data/support_data"

# Test directories
# PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
# TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Reviews_small"
# SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/support_data"
# review_sample_dir = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
# save_dropped_col_list = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

columns_to_drop = ['verified', 'reviewerID', 'reviewerName', 'reviewText', 'summary', 'style', 'image']
dropped_full_list = []
columns_order = ['asin', 'overall', 'vote', 'reviewTime', 'unixReviewTime']
for file in os.listdir(dir_load):
    filename = "/" + file
    df = getDF(dir_load + filename)
    columns_dropped = {file: []}
    for col in df.columns:
        if col in columns_to_drop:
            df.drop(col, axis=1, inplace=True)
            print(file + " dropping " + col)
            columns_dropped[file].append(col)
        else:
            pass
    dropped_full_list.append(columns_dropped)
    print('The columns in "{}" are now: {}'.format(file[0:-8], len(df.columns)))
    # set order of column dataframe
    df = df[columns_order]
    print(df.columns)
    df.to_json(dir_save + "/{}".format(file[:-3]), orient='records', lines=True)

writeToJSONFile(dir_save_support, 'reviews_skimming_dropped_columns', dropped_full_list)

print("Phew, that was a lot of work!")
