import pandas as pd
from fuzzywuzzy import fuzz
import numpy as np


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