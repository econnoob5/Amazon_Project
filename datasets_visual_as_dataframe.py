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



""" Set the relevant paths """

# extract project directory
project_dir = os.path.dirname(os.path.dirname(__file__))
# set directory to 'Metadata'
metadata_directory = os.path.join(project_dir, "Amazon Project - Data/Metadata_small")
# set directory to 'Reviews'
# reviews_directory = os.path.join(project_dir, "Amazon Project - Data/Reviews_small")
# set metadata file name to be opened. Paste file name in the blank "", do not edit "/"
file_metadata = "/" + "meta_Grocery_and_Gourmet_Food.json"
# set reviews file name to be opened. Paste file name in the blank "", do not edit "/"
# file_reviews = "/" + "All_Beauty.json"

""" To visualize data activate/deactivate the following two lines according to the needs """
# Open 'Metadata' files
df_meta = getDF(metadata_directory + file_metadata)

# Open 'Reviews' files
# df_reviews = getDF(reviews_directory + file_reviews)
