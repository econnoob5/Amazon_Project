"""""""""
Code PURPOSE: drop brand duplicates from 'Metadata' datasets to speed up the matching_firms_and_brands between the brands and the
              product names.

Code OUTPUT:

Code MECHANICS:
"""""""""

import pandas as pd
import json
import os


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


# The following function is use to save a json file to a directory
def writeToJSONFile(save_path, fileName, data):
    filePathNameWExt = save_path + '/' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp, indent=2)


""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# ----------------- test directories -----------------
# set PC test directories
# PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
# TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_load"
# SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_save"
# dir_load = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
# dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)


# ----------------- full run directories on IESE server -----------------
dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small"
dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_small-brand_nodup"

""""""""""""""""""""""""""""""""""""""""""""""""" Cleaning """""""""""""""""""""""""""""""""""""""""""""""""""""""""
i = 0
df_final = pd.DataFrame()
for file in os.listdir(dir_load):
    filename = "/" + file
    df = getDF(dir_load + filename)
    df_length = len(df['brand'])
    df_nodup = df.drop_duplicates(subset='brand')
    df_nodup_length = len(df_nodup['brand'])
    columns_dropped = df_length - df_nodup_length
    df_final = df_final.append(df_nodup, ignore_index=True)
    
    df_nodup.to_json(dir_save + "/{}".format(file), orient='records', lines=True)
    
    i += 1
    print('File {}, {}, processed. It has {} columns. {} columns dropped.'.format(i, file[:-5], len(df['brand']),
                                                                                 columns_dropped))
    print("")
    

df_final.to_csv(dir_save + "/metadata_all_small_brandnodup.csv")
print("metadata_all_small_brandnodup.csv has {} columns".format(len(df_final['brand'])))
print("END")