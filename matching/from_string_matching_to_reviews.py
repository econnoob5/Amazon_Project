"""""""""
Code PURPOSE: take the fuzzymatching output, link the metadata and link the reviews.
"""""""""

import pandas as pd
import json
import os

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

# matching_output = pd.read_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_final1.csv")
matching_output = pd.read_csv(r"/data/users/mlaudi/Amazon/all_matches.csv")
dir_load_metadata = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_small-brand_nodup"
dir_load_reviews = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Reviews_small"
dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results"

""""""""""""""""""""""""""""""""""""""""""""""""" Merging """""""""""""""""""""""""""""""""""""""""""""""""""""""""

merged_metadata_products = pd.DataFrame()
for file in os.listdir(dir_load_metadata):
    filename = "/" + file
    df = getDF(dir_load_metadata + filename)
    df_merged = pd.merge(df, matching_output, how='inner', left_on='brand', right_on='Brand')
    merged_metadata_products = merged_metadata_products.append(df_merged, ignore_index=True)
    print("{} - first loop".format(file))

merged_metadata_products_nodup = merged_metadata_products.drop_duplicates(subset='brand')
# unique_cusip = merged_metadata_products_nodup['cusip'].nunique()
# unique_products = merged_metadata_products_nodup['product_name'].nunique()

merged_metadata_products_reviews = pd.DataFrame()
for file_r in os.listdir(dir_load_reviews):
    filename_r = "/" + file_r
    df2 = getDF(dir_load_reviews + filename_r)
    df_merged2 = pd.merge(df2, merged_metadata_products_nodup, how='inner', on='asin')
    merged_metadata_products_reviews = merged_metadata_products_reviews.append(df_merged2, ignore_index=True)
    print("{} - second loop".format(file_r))

merged_metadata_products_nodup.to_csv(dir_save + "/metadata_products_fuzzymatching_fromallmatches.csv")
merged_metadata_products_nodup.to_excel(dir_save + "/metadata_products_fuzzymatching_fromallmatches.xlsx")
merged_metadata_products_reviews.to_csv(dir_save + "/metadata_products_reviews_fuzzymatching_fromallmatches.csv")
merged_metadata_products_reviews.to_excel(dir_save + "/metadata_products_reviews_fuzzymatching_fromallmatches.xlsx")

