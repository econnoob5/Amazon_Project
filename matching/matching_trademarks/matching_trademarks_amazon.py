import pandas as pd
import os
from fuzzywuzzy import fuzz
import time
import numpy as np
import scripts.functions.main_functions as MainFunc


""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# set PC directories
# dir_load_amz_brands = os.path.join(trademark_case_files_path, 'casefiles_owner_nodup_nonans.csv')
# dir_save = os.path.join(processed_data_dir, 'matching_firms_and_brands/match_trademarks_amazon')
# df_tdmks = pd.read_csv(dir_load_amz_brands, low_memory=False)

# server directories
# dir_load_amz_brands = r"/data/users/mlaudi/amazon_project/data/metadata_small_brandnodup"
# dir_load_tdmks = r"/data/users/mlaudi/" \
#                  r"amazon_project/data/cleaned/uspto/trademarks_case_files_dataset/casefiles_owner_nodup_nonans.csv"
# dir_save = r"/data/users/mlaudi/amazon_project/data/results/matching/trademarks"
# df_tdmks = pd.read_csv(dir_load_tdmks)

""""""""""""""""""""""""""""""""""""""""""""""""" Matching """""""""""""""""""""""""""""""""""""""""""""""""""""""""

# df to store row index, json file name, brand, product_name, fuzz ratio, fuzz partial_ratio
output_df_final = pd.DataFrame()
iteration_number = 0

for file in os.listdir(dir_load_amz_brands):

    iteration_number += 1

    filename = "/" + file
    df = MainFunc.get_df(dir_load_amz_brands + filename)  # function to open json file as df
    start_file = time.time()

    # First, we need the list of unique amazon brands and the list of factset products
    amzn_brands = df.brand.unique()

    # For every factset product, we create a variable called "len" that will compute the length of each string.
    # We do this bc we only want to compare amazon brands with trademarks that have similar length, so we need
    # to have this information.
    df_tdmks["len"] = df_tdmks["mark_id_char"].apply(lambda x: len(x))

    # On this list we will store the ultimate output, which at the end we will turn into a dataframe
    output_df_json_match, output_df_json_match_score, output_df_json_brand_list = [], [], []

    # output_df_json = pd.DataFrame(columns=["amazon", "factset", "wratio"])

    """ 
    The purpose of this function 'matching_firms_and_brands' is to compare a given 'brand_item' from the list of 
    'amz_brands' with the list of factset products whose length is similar to the amazon brand item length 
    (-2, +2 characters). We will apply the fuzz.ratio function using the map() method. 
    """


    def matching(brand_item, amazon_brands):

        global tdmks_and_ratios
        global matches
        global match
        global match_score
        global brand_item_list

        if brand_item is not None:
            # We create a new df that will only take those trademarks whose length is +4,-4 larger/shorter
            # than the length of the amazon brand.
            tdmks_name_length_range = df_tdmks[
                df_tdmks["len"].between(len(brand_item) - 4, len(brand_item) + 4)]["mark_id_char"].unique()

            # We apply fuzz.ratio using the map() method.
            # lambda x: x -> for every element in 'tdmks_name_length_range'.
            # Notice that we are passing only one amazon brand: 'brand_item'
            # which means that later we will have to apply this function to every element in 'amazon_brand'
            w_ratios = np.array(list(map(lambda x: fuzz.WRatio(x, brand_item), list(tdmks_name_length_range))))
            # The output will be a list of w_ratios, one for each element in tdmks_name_length_range

            if np.max(w_ratios, initial=1) > 80:  # We can play with this number

                # Once we have found that the max ratio is higher than the threshold, we want to find that "max" ratio
                # so that we can get the relevant factset product

                # Given that 'tdmks_name_length_range' and 'w_ratios' have the same length (as the second is built from
                # the first) we join them on a single dataframe
                tdmks_and_ratios = pd.DataFrame({"trademarks": tdmks_name_length_range, "wratio": w_ratios},
                                                columns=["trademarks", "wratio"])

                # We get the factset_product with the max ratio, it will be our match
                matches = tdmks_and_ratios[tdmks_and_ratios.wratio > 80]

                match = list(matches['trademarks'])
                match_score = list(matches['wratio'])
                brand_item_list = [brand_item] * len(matches)
                # brand_item_series = pd.Series(brand_item_list)

                # This simply computes the progress of the loop
                progress = round(np.where(amazon_brands == brand_item)[0][0] / len(amazon_brands) * 100, 2)

                print(
                    " File n.{} | {} | {}% of progress | {} | ".format(iteration_number,
                                                                       file[5:-5], progress, brand_item))

                # We append our results.
                output_df_json_match.append(match)
                output_df_json_match_score.append(match_score)
                output_df_json_brand_list.append(brand_item_list)
            else:

                pass  # If the max ratio is too low, we consider that there is no match


    # Using the map() method again, we apply the previous function (that depends on a single amazon brand item)
    # to all elements in 'azn_brands'
    list(map(lambda x: matching(x, amzn_brands), list(amzn_brands)))

    # We transform our output into a dataframe
    flattened_match = MainFunc.flatten(output_df_json_match)
    flattened_match_score = MainFunc.flatten(output_df_json_match_score)
    flattened_brand_list = MainFunc.flatten(output_df_json_brand_list)

    output_df_json = pd.DataFrame()
    output_df_json['Amazon'] = flattened_brand_list
    output_df_json['Factset'] = flattened_match
    output_df_json['WRatio'] = flattened_match_score
    output_df_json['category'] = file[5:-5]
    # append to final df
    output_df_final = output_df_final.append(output_df_json, ignore_index=True)