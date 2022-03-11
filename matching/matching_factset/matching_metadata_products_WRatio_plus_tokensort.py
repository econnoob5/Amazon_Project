""""""""""
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
import numpy as np
import matplotlib.pyplot as plt


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
# set PC directories
dir_load = r"../Amazon Project - Data/Metadata2/Metadata_small-brand_nodup"
dir_save = r"../../Amazon Project - Data/Results"
df_csv_file = pd.read_csv(r"../../Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv")

""""""""""""""""""""""""""""""""""""""""""""""""" Matching """""""""""""""""""""""""""""""""""""""""""""""""""""""""
# factset_companies = df_csv_file[["company name", "product_name"]]
output_df_final = pd.DataFrame()  # df to store row index, json file name, brand, product_name, fuzz ratio, fuzz partial_ratio
iteration_number = 0
for file in os.listdir(dir_load):

    iteration_number += 1

    filename = "/" + file
    df = getDF(dir_load + filename)  # function to open json file as df
    start_file = time.time()

    # First, we need the list of unique amazon brands and the list of factset products
    amazon_brands = df.brand.unique()
    factset_products = df_csv_file[["product_name"]].drop_duplicates()

    # For every factset product, we create a variable called "len" that will compute the length of each string.
    # We do this bc we only want to compare amazon brands with facteset brands that have similar length, so we need
    # to have this information.
    factset_products["len"] = factset_products["product_name"].apply(lambda x: len(x))

    # On this list we will store the ultimate output, which at the end we will turn into a dataframe
    output_df_json = []

    """ 
    The purpose of this function 'matching_firms_and_brands' is to compare a given 'brand_item' from the list of 'amazon_brands' with 
    the list of factset products whose length is similar to the amazon brand item length (-2, +2 characters).
    We will apply the fuzz.ratio function using the map() method. 
    """


    def matching(brand_item, amazon_brands):

        if brand_item != None:
            # We create a new df that will only take those factset products whose length is +4,-4 larger/shorter
            # than the length of the amazon brand.
            factset_products_list = factset_products[
                factset_products["len"].between(len(brand_item) - 4, len(brand_item) + 4)]["product_name"].unique()

            # We apply fuzz.ratio using the map() method.
            # lambda x: x -> for every element in 'factset_products_list'.
            # Notice that we are passing only one amazon brand: 'amazon_item'
            # which means that later we will have to apply this function to every element in 'amazon_brand'
            fwratio = np.array(list(map(lambda x: fuzz.WRatio(x, brand_item), list(factset_products_list))))
            fsortratio = np.array(list(map(lambda x: fuzz.token_sort_ratio(x, brand_item), list(factset_products_list))))
            fratios_zip = zip(fwratio, fsortratio)
            # The output will be a list of fwratio, one for each element in factset_products_list

            if np.max(fwratio, initial=1) > 80 or np.max(fsortratio, initial=1) > 80:  # We can play with this number

                # Once we have found that the max ratio is higher than this number, we want to find that "max" ratio
                # so that we can get the relevant factset product

                # Given that 'factset_products_list' and 'fwratio' have the same length (as the second is built from
                # the first) we join them on a single dataframe called 'a'
                a = pd.DataFrame({"factset_product": factset_products_list, "WRatio": fwratio, "sort_ratio": fwratio},
                                 columns=["factset_product", "ratio", "sort_ratio"])

                # We get the factset_product with the max ratio, it will be our match
                if np.max(fwratio, initial=1) > 80 and np.max(fsortratio, initial=1) > 80:
                    match = a.loc[a.WRatio == a.WRatio.max(), "factset_product"].values[0]

                    match_fwratio = a.loc[a.WRatio == a.WRatio.max(), "WRatio"].values[0]

                    match_sort_ratio = a.loc[a.WRatio == a.WRatio.max(), "sort_ratio"].values[0]

                elif not np.max(fsortratio, initial=1) > 80:
                    match = a.loc[a.WRatio == a.WRatio.max(), "factset_product"].values[0]

                    match_fwratio = a.loc[a.WRatio == a.WRatio.max(), "WRatio"].values[0]

                    match_sort_ratio = a.loc[a.WRatio == a.WRatio.max(), "sort_ratio"].values[0]

                # This simply computes the progress of the loop
                progress = round(np.where(amazon_brands == brand_item)[0][0] / len(amazon_brands) * 100, 2)

                print(
                    " File n.{} | {} | {}% of progress | {} -> {}, with WRatio {} sort_ratio {} | ".format(iteration_number,
                                                                                            file[5:-5], progress,
                                                                                            brand_item, match,
                                                                                            match_fwratio, match_sort_ratio))

                # We append our results.
                output_df_json.append({"amazon": brand_item, "factset": match, "WRatio": match_fwratio, "sort_ratio": match_sort_ratio})
            else:

                pass  # If the max ratio is too low, we consider that there is no match


    # Using the map() method again, we apply the previous function (that depends on a single amazon brand item)
    # to all elements in 'amazon_brands'
    list(map(lambda x: matching(x, amazon_brands), list(amazon_brands)))

    # We transform our output into a dataframe
    output_df_json = pd.DataFrame(output_df_json, columns=["amazon", "factset", "ratio"])
    output_df_json['category'] = file[5:-5]
    # append to final df
    output_df_final = output_df_final.append(output_df_json, ignore_index=True)

output_df_final.to_csv(r"../Amazon Project - Data/Results/matching_output_w_sort_ratios.csv", index=False)
output_df_final.to_excel(r"../Amazon Project - Data/Results/matching_output_w_sort_ratios.xlsx", index=False)


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# ======================================================================================================================
# ======================================================================================================================
#           ---------------------------   SCRAP CODE REPOSITORY FROM HERE ON   ---------------------------
# ======================================================================================================================
# ======================================================================================================================

# Directories
# PROJECT_ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + "\\"
# TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Metadata_small-brand_nodup"
# SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Results"
# dir_load = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
# dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)
# df_csv_file =  pd.read_csv(os.path.join(PROJECT_ROOT_DIR, r"Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv"))

# Save output_df_final
# output_df_final = pd.read_csv(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Results/matching_output_fratio.csv")
# get_permnos = pd.read_csv(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup_permnos.csv")
#
# get_these_permnos1 = pd.merge(output_df_final, get_permnos, how='right', left_on='factset', right_on='product_name')
#
# # write column to text file
# numpy_array = get_these_permnos1['LPERMNO'].to_numpy()
# np.savetxt("permnos_fromratio_forreturns.txt", numpy_array, fmt="%s")