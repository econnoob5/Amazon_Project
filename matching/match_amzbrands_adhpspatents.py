""""
Code PURPOSE: matching_firms_and_brands strings from USPTO Patent Assignment Dataset and the trademarks from the paper Autor et al.

Code OUTPUT:

Code MECHANICS:
"""""

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
# df_adhps = pd.read_stata(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Matching_procedure\merged_documentid_adhps.dta")
# dir_load = r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Metadata\Metadata_small-brand_nodup"
# dir_save = r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Tests\Metadata_test_save\trademarks_test"

# server directories
df_adhps = pd.read_stata(r"/data/users/mlaudi/Amazon/Amazon Project - Data/matching_firms_and_brands/trademarks/merged_documentid_adhps.dta")
dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small-brand_nodup"
dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_test_save/trademarks_test"

""""""""""""""""""""""""""""""""""""""""""""""""" Matching """""""""""""""""""""""""""""""""""""""""""""""""""""""""

df_trademarks = df_adhps[["title"]].drop_duplicates()

df_output = pd.DataFrame()  # df to store row index, json file name, brand, product_name, fuzz ratio, fuzz partial_ratio
iteration_number = 0
for file in os.listdir(dir_load):

    iteration_number += 1

    filename = "/" + file
    df_json = getDF(dir_load + filename)  # function to open json file as df_json
    start_file = time.time()

    # First, we need the list of unique amazon brands and the list of factset products
    amazon_brands = df_json.brand.unique()

    # For every trademark, we create a variable called "len" that will compute the length of each string.
    # We do this bc we only want to compare amazon brands with facteset brands that have similar length, so we need
    # to have this information.
    df_trademarks["len"] = df_trademarks["title"].apply(lambda x: len(x))

    # On this list we will store the ultimate output, which at the end we will turn into a dataframe
    output_df_json = []

    """
    The purpose of this function 'matching_firms_and_brands' is to compare a given 'brand_item' from the list of 'amazon_brands' with
    the list of trademarks whose length is similar to the amazon brand item length (-2, +2 characters).
    We will apply the fuzz.ratio function using the map() method.
    """


    def matching(brand_item, amazon_brands):

        if brand_item != None:
            # We create a new df that will only take those title whose length is +4,-4 larger/shorter
            # than the length of the amazon brand.
            l_trademarks = df_trademarks[
                df_trademarks["len"].between(len(brand_item) - 4, len(brand_item) + 4)]["title"].unique()

            # We apply fuzz.ratio using the map() method.
            # lambda x: x -> for every element in 'factset_products_list'.
            # Notice that we are passing only one amazon brand: 'amazon_item'
            # which means that later we will have to apply this function to every element in 'amazon_brand'
            ratios = np.array(list(map(lambda x: fuzz.ratio(x, brand_item), list(l_trademarks))))
            # The output will be a list of ratios, one for each element in factset_products_list

            if np.max(ratios, initial=1) > 80:  # We can play with this number

                # Once we have found that the max ratio is higher than this number, we want to find that "max" ratio
                # so that we can get the relevant factset product

                # Given that 'factset_products_list' and 'ratios' have the same length (as the second is built from
                # the first) we join them on a single dataframe called 'a'
                a = pd.DataFrame({"title": l_trademarks, "ratio": ratios},
                                 columns=["title", "ratio"])

                # We get the factset_product with the max ratio, it will be our match
                match = a.loc[a.ratio == a.ratio.max(), "title"].values[0]

                match_score = a.loc[a.ratio == a.ratio.max(), "ratio"].values[0]

                # This simply computes the progress of the loop
                progress = round(np.where(amazon_brands == brand_item)[0][0] / len(amazon_brands) * 100, 2)

                print(
                    " File n.{} | {} | {}% of progress | {} -> {}, with score {} | ".format(iteration_number,
                                                                                            file[5:-5], progress,
                                                                                            brand_item, match,
                                                                                            match_score, ))

                # We append our results.
                output_df_json.append({"amazon": brand_item, "tardemarks": match, "ratio": match_score})
            else:

                pass  # If the max ratio is too low, we consider that there is no match


    # Using the map() method again, we apply the previous function (that depends on a single amazon brand item)
    # to all elements in 'amazon_brands'
    list(map(lambda x: matching(x, amazon_brands), list(amazon_brands)))

    # We transform our output into a dataframe
    output_df_json = pd.DataFrame(output_df_json, columns=["amazon", "tardemarks", "ratio"])
    output_df_json['category'] = file[5:-5]
    # append to final df_json
    df_output = df_output.append(output_df_json, ignore_index=True)

# Save df_output
df_output.to_csv(dir_save + "/" + "trademarks_match_adhps_amazon.csv", index=False)
df_output.to_excel(dir_save  + "/" + "trademarks_match_adhps_amazon.xlsx", index=False)