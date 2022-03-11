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
                match = a.loc[a.wratio == a.wratio.max(), "factset_product"].values[0]

                match_score = a.loc[a.wratio == a.wratio.max(), "wratio"].values[0]

                # This simply computes the progress of the loop
                progress = round(np.where(amazon_brands == brand_item)[0][0] / len(amazon_brands) * 100, 2)

                print(
                    " File n.{} | {} | {}% of progress | {} -> {}, with score {} | ".format(iteration_number,
                                                                                            file[5:-5], progress,
                                                                                            brand_item, match,
                                                                                            match_score))

                # We append our results.
                output_df_json.append({"amazon": brand_item, "factset": match, "wratio": match_score})
            else:

                pass  # If the max ratio is too low, we consider that there is no match


    # Using the map() method again, we apply the previous function (that depends on a single amazon brand item)
    # to all elements in 'amazon_brands'
    list(map(lambda x: matching(x, amazon_brands), list(amazon_brands)))

    # We transform our output into a dataframe
    output_df_json = pd.DataFrame(output_df_json, columns=["amazon", "factset", "wratio"])
    output_df_json['category'] = file[5:-5]
    # append to final df
    output_df_final = output_df_final.append(output_df_json, ignore_index=True)

# Save output_df_final
output_df_final.to_csv(r"../Amazon Project - Data/Results/matching_output_fWRatio.csv", index=False)
output_df_final.to_excel(r"../Amazon Project - Data/Results/matching_output_fWRatio.xlsx", index=False)

x1 = []
x2 = []
x3 = []
y = []
for threshold in range(80, 101):
    thr = threshold
    filter1 = output_df_final.loc[output_df_final['wratio'] >= threshold]
    filter1_count1 = filter1['amazon'].nunique()
    filter1_count2 = filter1['factset'].nunique()
    filter1_count3 = filter1['category'].nunique()
    x1.append(filter1_count1)
    x2.append(filter1_count2)
    x3.append(filter1_count3)
    y.append(thr)

x1_series = pd.Series(x1, name='amazon')
x2_series = pd.Series(x2, name='factset')
y_series = pd.Series(y, name='fuzz.ratio')
ratio_table = pd.concat([x1_series, x2_series, y_series], axis=1)

plt.plot(x1, y, label='amazon')
plt.plot(x2, y, label='factset')
plt.xlabel('number of products')
plt.ylabel('fuzz.ratio scores')
plt.legend()
plt.grid(color='lightgray')

plt.show()

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    TEMPORARY CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

filter2 = output_df_final.loc[output_df_final['wratio'] >= 94]
filter2_2 = output_df_final.loc[output_df_final['wratio'] == 94]
filter2_count1 = filter2['amazon'].nunique()
filter2_count2 = filter2['factset'].nunique()
filter2_count3 = filter2['category'].nunique()

filter2 = filter2[~filter2.factset.isin(['U', 'A', 's', 'Ti', 'LU', 'G', 'RO', 'Q', 'FA'])]

filter2 = filter2.drop_duplicates(subset='amazon')
fWRmore_90_deleted = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project/Amazon Project - Data/Results/matching_output_fWRatio_morethan90_deleted.xlsx")
filter2.to_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project/Amazon Project - Data/Results/matching_output_fWRatio_morethan94.xlsx", index=False)

temp_mask = filter2.factset.isin(fWRmore_90_deleted)
filter2 = filter2[~temp_mask].reset_index()

fWRmore_98 = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project/Amazon Project - Data/Results/matching_output_fWRatio_morethan98.xlsx")
fRatiomore_90 = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project/Amazon Project - Data/Results/matching_output_fratio_morethan90.xlsx")

ratios_merged = pd.merge(fWRmore_98, fRatiomore_90, how='inner', on='amazon')

mask1 = fWRmore_98.factset.isin(fRatiomore_90.factset)
fWR_masked_not = fWRmore_98[~mask1].reset_index()
mask2 = fWRmore_98.amazon.isin(fRatiomore_90.amazon)
fWR_masked2 = fWRmore_98[~mask2].reset_index()


final_ratios = fRatiomore_90
final_ratios = final_ratios.append(fWR_masked_not)



"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# ======================================================================================================================
# ======================================================================================================================
#           ---------------------------   SCRAP CODE REPOSITORY FROM HERE ON   ---------------------------
# ======================================================================================================================
# ======================================================================================================================