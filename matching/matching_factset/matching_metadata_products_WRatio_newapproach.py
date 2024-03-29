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


def flatten(t):
    return [item for sublist in t for item in sublist]

""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# set PC directories
# dir_load = r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Metadata\Metadata_small-brand_nodup"
# dir_save = r"../Amazon Project - Data/Results"
# df_csv_file = pd.read_csv(r"../Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv")

# server directories
dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small-brand_nodup"
dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results"
df_csv_file = pd.read_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup.csv")

""""""""""""""""""""""""""""""""""""""""""""""""" Matching """""""""""""""""""""""""""""""""""""""""""""""""""""""""

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
    output_df_json_match = []
    output_df_json_match_score = []
    output_df_json_brand_list = []
    # output_df_json = pd.DataFrame(columns=["amazon", "factset", "wratio"])

    """ 
    The purpose of this function 'matching_firms_and_brands' is to compare a given 'brand_item' from the list of 'amazon_brands' with 
    the list of factset products whose length is similar to the amazon brand item length (-2, +2 characters).
    We will apply the fuzz.ratio function using the map() method. 
    """


    def matching(brand_item, amazon_brands):

        global a
        global matches
        global match
        global match_score
        global brand_item_list

        if brand_item != None:
            # We create a new df that will only take those factset products whose length is +4,-4 larger/shorter
            # than the length of the amazon brand.
            factset_products_list = factset_products[
                factset_products["len"].between(len(brand_item) - 4, len(brand_item) + 4)]["product_name"].unique()

            # We apply fuzz.ratio using the map() method.
            # lambda x: x -> for every element in 'factset_products_list'.
            # Notice that we are passing only one amazon brand: 'brand_item'
            # which means that later we will have to apply this function to every element in 'amazon_brand'
            wratios = np.array(list(map(lambda x: fuzz.WRatio(x, brand_item), list(factset_products_list))))
            # The output will be a list of wratios, one for each element in factset_products_list

            if np.max(wratios, initial=1) > 80:  # We can play with this number

                # Once we have found that the max ratio is higher than this number, we want to find that "max" ratio
                # so that we can get the relevant factset product

                # Given that 'factset_products_list' and 'wratios' have the same length (as the second is built from
                # the first) we join them on a single dataframe called 'a'
                a = pd.DataFrame({"factset_product": factset_products_list, "wratio": wratios},
                                 columns=["factset_product", "wratio"])

                # We get the factset_product with the max ratio, it will be our match
                matches = a[a.wratio > 80]

                match = list(matches['factset_product'])
                match_score = list(matches['wratio'])
                brand_item_list = [brand_item] * len(matches)
                # brand_item_series = pd.Series(brand_item_list)

                # This simply computes the progress of the loop
                progress = round(np.where(amazon_brands == brand_item)[0][0] / len(amazon_brands) * 100, 2)

                print(
                    " File n.{} | {} | {}% of progress | {} | ".format(iteration_number,file[5:-5], progress,brand_item))

                # We append our results.
                output_df_json_match.append(match)
                output_df_json_match_score.append(match_score)
                output_df_json_brand_list.append(brand_item_list)
            else:

                pass  # If the max ratio is too low, we consider that there is no match


    # Using the map() method again, we apply the previous function (that depends on a single amazon brand item)
    # to all elements in 'amazon_brands'
    list(map(lambda x: matching(x, amazon_brands), list(amazon_brands)))

    # We transform our output into a dataframe
    flattened_brand_list = flatten(output_df_json_brand_list)
    flattened_match = flatten(output_df_json_match)
    flattened_match_score = flatten(output_df_json_match_score)

    output_df_json = pd.DataFrame()
    output_df_json['Amazon'] = flattened_brand_list
    output_df_json['Factset'] = flattened_match
    output_df_json['WRatio'] = flattened_match_score
    output_df_json['category'] = file[5:-5]
    # append to final df
    output_df_final = output_df_final.append(output_df_json, ignore_index=True)
#
#     else:
#         break

df1 = output_df_final.iloc[:500000, :]
df2 = output_df_final.iloc[500001:1000000, :]
df3 = output_df_final.iloc[1000001:1500000, :]
df4 = output_df_final.iloc[1500001:2000000, :]
df5 = output_df_final.iloc[2000001:, :]

# Save output_df_final
df1.to_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_1.csv", index=False)
df1.to_excel(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_1.xlsx", index=False)

df2.to_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_2.csv", index=False)
df2.to_excel(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_2.xlsx", index=False)

df3.to_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_3.csv", index=False)
df3.to_excel(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_3.xlsx", index=False)

df4.to_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_4.csv", index=False)
df4.to_excel(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_4.xlsx", index=False)

df5.to_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_5.csv", index=False)
df5.to_excel(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2_5.xlsx", index=False)

# output_df_final.to_csv(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2.csv", index=False)
# output_df_final.to_excel(r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/matching_output_fWRatio_approach2.xlsx", index=False)