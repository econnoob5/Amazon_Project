"""""""""
Code PURPOSE: get the csv files resulting from the scraping of Amazon, done to get additional brands to add to the UCSD 
              products to expand the number of brands we have, and perform fuzzymatching between the product title from 
              UCSD and the product title gotten from the scraping. If the matching_firms_and_brands score is high enough we keep the 
              match to get the brand, if not we discard it.
              
Code not complete!

"""""""""


import pandas as pd
import os
import time
import numpy as np
from fuzzywuzzy import fuzz

csv_file = pd.read_csv(r"../../Amazon Project - Data/Results/Amazon scraping/meta_All_Beauty.csv")

title_from_amz_dataset = csv_file.title.tolist()
product_caption = csv_file.product_caption.tolist()
zippy = list(zip(title_from_amz_dataset, product_caption))


def matching(tuple):

    global elem1
    global elem2
    global ratio

    elem1 = tuple[0]
    elem2 = tuple[1]
    try:
        ratio = fuzz.WRatio(elem1, elem2)
        list_ratio = [elem1, elem2, ratio]
        series_ratio = pd.Series(list_ratio)
        return series_ratio
    except ValueError:
        list_elements = [elem1, elem2]
        series_errors = pd.Series(list_elements)
        return series_errors
    # return series_ratio, series_errors

final_df = pd.DataFrame()
for t in zippy:
    series_matching = matching(t)
    final_df = final_df.append(series_matching, ignore_index=True)
    print(series_matching)


    
#     ratio = np.array(list(map(lambda x: fuzz.WRatio(x, title), list(product_caption))))
#
#     if np.max(wratios, initial=1) > 80:  # We can play with this number
#
#
# output_df_final = pd.DataFrame()  # df to store row index, json file name, brand, product_name, fuzz ratio, fuzz partial_ratio
# iteration_number = 0
# for file in os.listdir(dir_load):
#
#     iteration_number += 1
#
#     filename = "/" + file
#     df = getDF(dir_load + filename)  # function to open json file as df
#     start_file = time.time()
#
#     # First, we need the list of unique amazon brands and the list of factset products
#     amazon_brands = df.brand.unique()
#     factset_products = df_csv_file[["product_name"]].drop_duplicates()
#
#     # For every factset product, we create a variable called "len" that will compute the length of each string.
#     # We do this bc we only want to compare amazon brands with facteset brands that have similar length, so we need
#     # to have this information.
#     factset_products["len"] = factset_products["product_name"].apply(lambda x: len(x))
#
#     # On this list we will store the ultimate output, which at the end we will turn into a dataframe
#     output_df_json = []
#
#     """
#     The purpose of this function 'matching_firms_and_brands' is to compare a given 'brand_item' from the list of 'amazon_brands' with
#     the list of factset products whose length is similar to the amazon brand item length (-2, +2 characters).
#     We will apply the fuzz.ratio function using the map() method.
#     """
#
#
#     def matching_firms_and_brands(brand_item, amazon_brands):
#
#         if brand_item != None:
#             # We create a new df that will only take those factset products whose length is +4,-4 larger/shorter
#             # than the length of the amazon brand.
#             factset_products_list = factset_products[
#                 factset_products["len"].between(len(brand_item) - 4, len(brand_item) + 4)]["product_name"].unique()
#
#             # We apply fuzz.ratio using the map() method.
#             # lambda x: x -> for every element in 'factset_products_list'.
#             # Notice that we are passing only one amazon brand: 'amazon_item'
#             # which means that later we will have to apply this function to every element in 'amazon_brand'
#             wratios = np.array(list(map(lambda x: fuzz.WRatio(x, brand_item), list(factset_products_list))))
#             # The output will be a list of wratios, one for each element in factset_products_list
#
#             if np.max(wratios, initial=1) > 80:  # We can play with this number
#
#                 # Once we have found that the max ratio is higher than this number, we want to find that "max" ratio
#                 # so that we can get the relevant factset product
#
#                 # Given that 'factset_products_list' and 'wratios' have the same length (as the second is built from
#                 # the first) we join them on a single dataframe called 'a'
#                 a = pd.DataFrame({"factset_product": factset_products_list, "wratio": wratios},
#                                  columns=["factset_product", "wratio"])
#
#                 # We get the factset_product with the max ratio, it will be our match
#                 match = a.loc[a.wratio == a.wratio.max(), "factset_product"].values[0]
#
#                 match_score = a.loc[a.wratio == a.wratio.max(), "wratio"].values[0]
#
#                 # This simply computes the progress of the loop
#                 progress = round(np.where(amazon_brands == brand_item)[0][0] / len(amazon_brands) * 100, 2)
#
#                 print(
#                     " File n.{} | {} | {}% of progress | {} -> {}, with score {} | ".format(iteration_number,
#                                                                                             file[5:-5], progress,
#                                                                                             brand_item, match,
#                                                                                             match_score))
#
#                 # We append our results.
#                 output_df_json.append({"amazon": brand_item, "factset": match, "wratio": match_score})
#             else:
#
#                 pass  # If the max ratio is too low, we consider that there is no match
#
#
#     # Using the map() method again, we apply the previous function (that depends on a single amazon brand item)
#     # to all elements in 'amazon_brands'
#     list(map(lambda x: matching_firms_and_brands(x, amazon_brands), list(amazon_brands)))
#
#     # We transform our output into a dataframe
#     output_df_json = pd.DataFrame(output_df_json, columns=["amazon", "factset", "wratio"])
#     output_df_json['category'] = file[5:-5]
#     # append to final df
#     output_df_final = output_df_final.append(output_df_json, ignore_index=True)
#
# # Save output_df_final
# output_df_final.to_csv(r"../Amazon Project - Data/Results/matching_output_fWRatio.csv", index=False)
# output_df_final.to_excel(r"../Amazon Project - Data/Results/matching_output_fWRatio.xlsx", index=False)
