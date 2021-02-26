"""
Code PURPOSE: The purpose of the following code is to merge the firms coming from compustat North America fundamentals
              annual, list of which was cleaned in the code 'firms_list_compustat_cleaning.py', with their corresponding
              products coming from Factset Revere - Products.

Code OUTPUT:

Code MECHANICS: simple merging between the products from Factset Revere and the list of firms from Compustat
"""

import pandas as pd
import os
import openpyxl

""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
TARGET_FOLDER = r"Amazon Project - Data/firms_products_link"
TARGET_FILE = r"Amazon Project - Data/firms_products_link/firms_products_skimmed.csv"
dir_load = os.path.join(PROJECT_ROOT_DIR, TARGET_FILE)
dir_save = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER)

# load csv file
products = pd.read_csv(dir_load)

# dropp duplicates in product name column
products_nodup = products.drop_duplicates(subset='product_name')

# save to excel and csv
products_nodup.to_csv(dir_save + "/firms_products_skimmed_nodup.csv", index=False)
products_nodup.to_excel(dir_save + "/firms_products_skimmed_nodup.xlsx", index=False)
print("END")