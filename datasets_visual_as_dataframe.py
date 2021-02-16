import pandas as pd
import gzip
import json
import os


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


""" Set the relevant paths """

# extract project directory
project_dir = os.path.dirname(os.path.dirname(__file__))
# set directory to 'Metadata'
metadata_directory = os.path.join(project_dir, "Amazon Project - Data/Metadata")
# set directory to 'Reviews'
reviews_directory = os.path.join(project_dir, "Amazon Project - Data/Reviews")
# set metadata file name to be opened. Paste file name in the blank "", do not edit "/"
file_metadata = "/" + "meta_Home_and_Kitchen.json.gz"
# set reviews file name to be opened. Paste file name in the blank "", do not edit "/"
file_reviews = "/" + "All_Beauty.json.gz"

""" To visualize data activate/deactivate the following two lines according to the needs """
# Open 'Metadata' files
df_meta = getDF(metadata_directory + file_metadata)

# Open 'Reviews' files
df_reviews = getDF(reviews_directory + file_reviews)
