import pandas as pd
import os
import json

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
# dir_load_meta = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_small-brand_nodup"
# dir_load_reviews = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Reviews_small"
# dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results"
# csv_file = r"/data/users/mlaudi/Amazon/Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv"

# set PC directories
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
TARGET_FOLDER_ROOT_DIR_META = r"Amazon Project - Data\Metadata_small-brand_nodup"
TARGET_FOLDER_ROOT_DIR_REVIEWS = r"Amazon Project - Data/Reviews_small"
SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data\Results"

dir_load_meta = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR_META)
dir_load_reviews = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR_REVIEWS)
dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

csv_file = os.path.join(PROJECT_ROOT_DIR + "Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv")

""""""""""""""""""""""""""""""""""""""""""""""""" Merging """""""""""""""""""""""""""""""""""""""""""""""""""""""""

df_csv_file = pd.read_csv(csv_file)

merged_metadata_products = pd.DataFrame()
for file in os.listdir(dir_load_meta):
    filename = "/" + file
    df = getDF(dir_load_meta + filename)
    df_merged = pd.merge(df, df_csv_file, how='inner', left_on='brand', right_on='product_name')
    merged_metadata_products = merged_metadata_products.append(df_merged, ignore_index=True)
    print("{} - first loop".format(file))

merged_metadata_products_nodup = merged_metadata_products.drop_duplicates(subset='brand')
unique_cusip = merged_metadata_products_nodup['cusip'].nunique()
unique_products = merged_metadata_products_nodup['product_name'].nunique()

merged_merged_reviews = pd.DataFrame()
for file_r in os.listdir(dir_load_reviews):
    filename_r = "/" + file_r
    df2 = getDF(dir_load_reviews + filename_r)
    df_merged2 = pd.merge(df2, merged_metadata_products_nodup, how='inner', on='asin')
    merged_merged_reviews = merged_merged_reviews.append(df_merged2, ignore_index=True)
    print("{} - second loop".format(file_r))

merged_metadata_products_nodup.to_csv(dir_save + "/metadata_products_hardmerge.csv")
merged_metadata_products_nodup.to_excel(dir_save + "/metadata_products_hardmerge.xlsx")
merged_merged_reviews.to_csv(dir_save + "/metadata_products_reviews_hardmerge.csv")
merged_merged_reviews.to_excel(dir_save + "/metadata_products_reviews_hardmerge.xlsx")