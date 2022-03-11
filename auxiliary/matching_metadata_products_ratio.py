"""""""""
Code PURPOSE: prototype code for matching_firms_and_brands amazon brands with Factset products

Code OUTPUT:

Code MECHANICS: define first the usual functions to parse the json files. Then load the directories. Then we perform the
                matching_firms_and_brands between brand names from the Amazon datasets and the product names from Factset Revere
"""""""""

import pandas as pd
import json
import os
from fuzzywuzzy import fuzz
import time
import numpy as np


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


_loop = False


df_ratios = pd.DataFrame()  # df to store row index, json file name, brand, product_name, fuzz ratio, fuzz partial_ratio
df_iterations = pd.DataFrame()  # df to store how long it took to loop all the way through a json file

for file in os.listdir(dir_load):
    filename = "/" + file
    df = getDF(dir_load + filename)  # function to open json file as df
    start_file = time.time()
    
    
    """ NEW METHOD STARTS HERE ------------------------------------------------------------------------------------------------ """
    
    
    if not _loop:  # I created this _loop variable to switch off/onn this new method using map(), but we can delete it

        # First, we need the list of unique amazon brands and the list of factset products
        amazon_brands = df.brand.unique()
        factset_products = df_csv_file[["product_name"]].drop_duplicates()

        # For every factset product, we create a variable called "len" that will compute the length of each string.
        # We do this bc we only want to compare amazon brands with facteset brands that have similar length, so we need
        # to have this information.
        factset_products["len"] = factset_products["product_name"].apply(lambda x: len(x))

        # On this list we will store the ultimate output, which at the end we will turn into a dataframe
        output_df = []

        """ 
        The purpose of this function 'matching_firms_and_brands' is to compare a given 'brand_item' from the list of 'amazon_brands' with 
        the list of factset products whose length is similar to the amazon brand item length (-2, +2 characters).
        We will apply the fuzz.ratio function using the map() method. 
        """
        def matching(brand_item, amazon_brands):

            if brand_item != None:
                # We create a new df that will only take those factset products whose length is +2,-2 larger/shorter
                # than the length of the amazon brand.
                factset_products_list = factset_products[
                    factset_products["len"].between(len(brand_item)-2, len(brand_item)+2)]["product_name"].unique()

                # We apply fuzz.ratio using the map() method.
                # lambda x: x -> for every element in 'factset_products_list'.
                # Notice that we are passing only one amazon brand: 'amazon_item'
                # which means that later we will have to apply this function to every element in 'amazon_brand'
                ratios = np.array(list(map(lambda x: fuzz.ratio(x, brand_item), list(factset_products_list))))
                # The output will be a list of ratios, one for each element in factset_products_list

                # Returning output...

                if np.max(ratios) > 80: # We can play with this number
                    # Once we have found that the max ratio is higher than this number, we want to find that "max" ratio
                    # so that we can get the relevant factset product

                    # Given that 'factset_products_list' and 'ratios' have the same lenght (as the second is built from
                    # the first) we join them on a single dataframe called 'a'
                    a = pd.DataFrame({"factset_product": factset_products_list, "ratio": ratios}, columns=["factset_product", "ratio"])

                    # We get the factset_product with the max ratio, it will be our match
                    match = a.loc[a.ratio == a.ratio.max(), "factset_product"].values[0]

                    # This simply computes the progress of the loop
                    progress = round(np.where(amazon_brands == brand_item)[0][0]/len(amazon_brands)*100, 2)

                    print(" {} -> {}, {}% of progress".format(brand_item, match, progress))

                    # We append our results.
                    output_df.append({"amazon": brand_item, "factset": match})
                else:
                    pass # If the max ratio is too low, we consider that there is no match

        
        # Using the map() method again, we apply the previous function (that depends on a single amazon brand item) 
        # to all elements in 'amazon_brands'
        list(map(lambda x: matching(x,amazon_brands),list(amazon_brands)))
        
        # We transform our output into a dataframe
        output_df = pd.DataFrame(output_df, columns=["amazon", "factset"])
