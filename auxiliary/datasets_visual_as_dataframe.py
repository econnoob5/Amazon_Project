"""
Code PURPOSE: visualize json files from UCSD Amazon database, 'Metadata' and 'Reviews'

Code OUTPUT: df_meta and df_reviews. No file is saved here, they remain in RAM just for visualisation

Code MECHANICS: use two functions to open the jsons as df and then assign to the function getDF the right directory
"""

import pandas as pd
import gzip
import json
import os

"""
IMPORTANT: turn on or off 'parse' and 'getDF' functions depending on whether the files to be read are gzipped or not.
"""

# The following two functions are used to parse the GZIPPED json files and to open the as a Pandas DataFrame
# def parse(path):
#     g = gzip.open(path, 'rb')
#     for l in g:
#         yield json.loads(l)
#
#
# def getDF(path):
#     i = 0
#     df = {}
#     for d in parse(path):
#         df[i] = d
#         i += 1
#     return pd.DataFrame.from_dict(df, orient='index')

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
# extract project directory
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
# set directory to 'Metadata'
metadata_directory = os.path.join(PROJECT_ROOT_DIR, "Amazon Project - Data/Metadata/Metadata_small_brand_nan")
# set directory to 'Reviews'
reviews_directory = os.path.join(PROJECT_ROOT_DIR, "Amazon Project - Data/Reviews/Reviews_small")
# set metadata file name to be opened. Paste file name in the blank "", do not edit "/"
file_metadata = "/" + "meta_All_Beauty.json"
# set reviews file name to be opened. Paste file name in the blank "", do not edit "/"
# file_reviews = "/" + "All_Beauty.json"
dir_load = r"../../Amazon Project - Data/Results/Amazon scraping/meta_Office_Products.csv"

""""""""""""""""""""""""""""""""""""""""""" Open files as DataFrames """""""""""""""""""""""""""""""""""""""""""""""""""
# Open 'Metadata' files
df_meta = getDF(metadata_directory + file_metadata)

# Open 'Reviews' files
# df_reviews = getDF(reviews_directory + file_reviews)

# Select snippets of dataframes
#df_snippet = df_meta.drop(columns=['description', 'title'])
#df_snippet.to_stata(r'../Amazon Project - Data/support_data/To send to Miguel/Examples/All_Beauty_nodup.dta', version=117)

# read CSV and EXCEL 
excel_file = pd.read_csv(dir_load)