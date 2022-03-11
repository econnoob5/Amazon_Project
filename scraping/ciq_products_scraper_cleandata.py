import re
import pandas as pd
import os
from fuzzywuzzy import fuzz
import numpy as np

folder_directory = r"../Amazon Project - Data/firms_products_link/products_from_ciq/"

df_count = pd.DataFrame()
df_for_matching = pd.DataFrame()
df_fileswithissues = pd.DataFrame()
count = 0
products_number_count = 0

for filename in os.listdir(folder_directory):

    try:
        # open df_file as pd
        df_file = pd.read_excel(folder_directory + '{}'.format(filename))
        # drop first 10 rows
        df_file = df_file.iloc[11:].reset_index(drop=True)
        # set column headers = to first row
        df_file.columns = df_file.iloc[0]
        # drop first row used for column headers
        df_file = df_file.iloc[1:]
        # add column with parent company
        df_file['Parent company'] = filename[:-13]
        # count products
        products_number = df_file['Product Name'].nunique()
        products_number_count += products_number
        list_temp_prod_number = [filename, int(products_number)]
        series_temp_prod_number = pd.Series(list_temp_prod_number)
        df_count = df_count.append(series_temp_prod_number, ignore_index=True)

    #   extract info we need for matching_firms_and_brands
        list_columns = ['Product Name', 'Company', 'Parent company']
        df_temp_for_matching = df_file[df_file.columns & list_columns]
        # df_temp_for_matching = df_file[df_file.index.intersection(list_columns)]
        df_for_matching = df_for_matching.append(df_temp_for_matching, ignore_index=True)

    except:
        list_temp_filewithissue = [filename]
        series_temp_filewithissue = pd.Series(list_temp_filewithissue)
        df_fileswithissues = df_fileswithissues.append(series_temp_filewithissue, ignore_index=True)


df_fileswithissues.rename(columns={df_fileswithissues.columns[0]: 'Company'}, inplace=True)
df_for_matching.to_feather(r"../Amazon Project - Data/firms_products_link/df_for_matching_ciq.ftr")
df_fileswithissues.to_feather(r"../Amazon Project - Data/firms_products_link/df_matching_issues_ciq.ftr")

featherfile = pd.read_feather(r"../../Amazon Project - Data/firms_products_link/df_for_matching_ciq.ftr")