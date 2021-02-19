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
server_target_dir = r"/data/users/mlaudi/Amazon/Data/Reviews"
server_save_dir = r"/data/users/mlaudi/Amazon/Data/Reviews_small"
server_support_data_dir = r"/data/users/mlaudi/Amazon/Data/support_data"

# Test directories
# PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
# TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Reviews_small"
# SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/support_data"
# review_sample_dir = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
# save_dropped_col_list = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

columns_to_drop = ['verified', 'reviewTime', 'reviewerID', 'reviewerName', 'reviewText', 'summary', 'style', 'image']
dropped_full_list = []
columns_order = ['asin', 'overall', 'vote', 'unixReviewTime']
for file in os.listdir(server_target_dir):
    filename = "/" + file
    df = getDF(server_target_dir + filename)
    columns_dropped = {file : []}
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
    df.to_json(server_save_dir + "/{}".format(file[:-3]), orient='records', lines=True)

writeToJSONFile(server_support_data_dir, 'reviews_skimming_dropped_columns', dropped_full_list)

print("Phew, that was a lot of work!")