"""""""""""
Code PURPOSE: the code counts 'nans' in metadata json files. The most relevant column is 'brand', as it is important to
              understand if we can directly work with it for the matching_firms_and_brands or some preliminary work is necessary.

Code OUTPUT: Amazon Project\Amazon Project - Data\support_data\count_of_nans_brandcol.xlsx

Code MECHANICS: the following code loads the metadata json files from the folder 'Metadata_small' to compute the number
                of nans in each columns.
"""""""""""

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
# server_load_dir_test = r"/data/users/mlaudi/Amazon/Data/Tests/metadata_test_load"
# server_save_dir_test = r"/data/users/mlaudi/Amazon/Data/Tests/metadata_test_save"

# server full run directories
server_load_dir = r"/data/users/mlaudi/Amazon/Data/Metadata_small"
server_save_support = r"/data/users/mlaudi/Amazon/Data/support_data"

# set PC test directories
# PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
# TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/metadata_test_load"
# SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/metadata_test_save"
# metadata_sample_dir = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
# save_dir = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

nans_in_dfs = pd.DataFrame()
for file in os.listdir(server_load_dir):
    filename = "/" + file
    df = getDF(server_load_dir + filename)
    df = df.fillna(value=np.nan)
    df_number_of_nans = df.isna().sum()
    df_number_rows = len(df['brand'])

    # the following for-loop adds percentage of nans over the total
    temp_nans = pd.Series(df_number_of_nans, name=file[:-5])
    for index, row in temp_nans.iteritems():
        percent = round((row / df_number_rows) * 100, 2)
        string_to_add = str(row) + " (" + str(percent) + "%)"
        temp_nans.replace(row, string_to_add, inplace=True)

    # add column with number of rows in each of the metadata files
    temp_rows_dict = {'Rows Number': df_number_rows}
    temp_rows = pd.Series(temp_rows_dict, name='Rows Number')
    temp_nans = temp_nans.append(temp_rows)
    temp_nans.name = file[
                     :-5]  # had to change the name of Series again because when appending 'temp_rows' to 'temp_nans'
    # the name of the series would change from the name of the file to None, throwing then an
    # error in the next step when appending to the dataframe

    nans_in_dfs = nans_in_dfs.append(temp_nans, ignore_index=False)

    print("{} done".format(file[:-5]))

nans_in_dfs.to_excel(server_save_support + "/count_of_nans_brandcol.xlsx")
