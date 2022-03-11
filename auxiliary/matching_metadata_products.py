"""""""""""
Code PURPOSE: match 'brand' in Amazon datasets with 'product name' from Factset Revere. We need a fuzzy matching_firms_and_brands as the
              strings may vary greatly amongst each other.

Code OUTPUT:

Code MECHANICS: define first the usual functions to parse the json files. Then load the directories. Then we perform the
                matching_firms_and_brands between brand names from the Amazon datasets and the product names from Factset Revere
"""""""""""

import pandas as pd
import json
import os
from fuzzywuzzy import fuzz
import time


# The following two functions are used to parse the NOT-GZIPPED json files and to open the as a Pandas DataFrame
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
# set server test directories
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_test_load"
# dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_test_save"
# csv_file = r"/data/users/mlaudi/Amazon/Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv"

# set PC test directories
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Metadata_small-brand_nodup"
SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_save"
dir_load = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)
csv_file = os.path.join(PROJECT_ROOT_DIR + "Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv")

# ----------------- full run directories on IESE server -----------------
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small"
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/support_data"
# csv_file = r"/data/users/mlaudi/Amazon/Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv"

""""""""""""""""""""""""""""""""""""""""""""""""" Matching """""""""""""""""""""""""""""""""""""""""""""""""""""""""
# load csv-file as pandas df
df_csv_file = pd.read_csv(csv_file)

df_ratios = pd.DataFrame()  # df to store row index, json file name, brand, product_name, fuzz ratio, fuzz partial_ratio
df_iterations = pd.DataFrame()  # df to store how long it took to loop all the way through a json file
for file in os.listdir(dir_load):
    filename = "/" + file
    df = getDF(dir_load + filename)  # function to open json file as df
    start_file = time.time()

    i = -1  # useful for timing the for-loop
    list_time = []  # store for-loop times
    for row in df.itertuples():
        brand = row[3]  # get the brand name from Amazon dataset

        # the following if statement computes the timing to loop through 10 rows in the DataFrame
        i += 1
        if (i % 10) == 0:
            timing = time.time()
            list_time.append(timing)
            if len(list_time) == 2:
                iter_time = list_time[1] - list_time[0]
                print("{}, line {} - 10 rows in {} seconds ".format(file[:-5], row[0], iter_time))
                del list_time[0]
            else:
                pass
        else:
            pass

        # here we compute the fuzzy ratios for each brand in row (from each Amazon category) with each product name in
        # the dataset from Factset Revere that links companies with their products between 2003 and 2018
        for line in df_csv_file.itertuples():
            product_name = line[5]
            fuzz_ratio = fuzz.ratio(brand, product_name)
            fuzz_partial_ratio = fuzz.partial_ratio(brand, product_name)

            if fuzz_ratio > 60 and fuzz_partial_ratio > 70:
                tuple_ratios = (
                str(row[0]), file[:-5], str(brand), str(product_name), str(fuzz_ratio), str(fuzz_partial_ratio))
                series_tuple = pd.Series(tuple_ratios)
                df_ratios = df_ratios.append(series_tuple, ignore_index=True)
                print("{}".format(tuple_ratios))
            else:
                pass

    # compute time to loop through the file and append to df_iterations, saving dataset name and minutes.
    # Might be useful to study the efficiency of the matching_firms_and_brands
    end_file = time.time()
    iteration_time_file = (end_file - start_file) / 60
    time_tuple = (file[:-5], iteration_time_file)
    temp_iteration = pd.Series(time_tuple)
    df_iterations = df_iterations.append(temp_iteration, ignore_index=True)
    print("- - - - - - - - - - - - - {} took {} minutes - - - - - - - - - - - -".format(file[:-5], iteration_time_file))

# save to server test directories
# df_ratios.to_excel(r"/data/users/mlaudi/Amazon/Data/Tests/Metadata_test_save/ratios_brand_prodname.xlsx")
# df_iterations.to_excel(r"/data/users/mlaudi/Amazon/Data/Tests/Metadata_test_save/iterations_time_test.xlsx")
