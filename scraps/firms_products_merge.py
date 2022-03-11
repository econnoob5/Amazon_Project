"""""""""
Code PURPOSE: The purpose of the following code is to merge the firms coming from Compustat North America Fundamentals
              Annual, list of which was cleaned in the code 'firms_list_compustat_cleaning.py', with their corresponding
              products coming from Factset Revere - Products.

Code OUTPUT: - Amazon Project\Amazon Project - Data\firms_products_link\firms_products_allcolumns.csv
             - Amazon Project\Amazon Project - Data\firms_products_link\firms_products_allcolumns.xlsx
             - Amazon Project\Amazon Project - Data\firms_products_link\firms_products_skimmed.csv
             - Amazon Project\Amazon Project - Data\firms_products_link\firms_products_skimmed.xlsx

Code MECHANICS: simple merging between the products from Factset Revere and the list of firms from Compustat
"""""""""

import pandas as pd

# load list of products from factset
products = pd.read_csv(
    r"../../Amazon Project - Data/Products Lists/factset_all_products.csv")

# load list of firms from compustat
firms = pd.read_csv(
    r"../../Amazon Project - Data/Firms List/Firms list compustat/list_of_firms_compustat.csv")

# merge the factset and compustat together
target_df = pd.merge(products, firms, how='inner', on='cusip').rename(columns={'conm': 'company name'})

target_df_skimmed = target_df.drop(
    columns=['start_', 'end_', 'name', 'active', 'isin', 'Unnamed: 0', 'gvkey', 'datadate',
             'fyear', 'indfmt', 'consol', 'popsrc', 'datafmt', 'tic', 'curcd', 'costat'], axis=1)

# compute two simple statistics for general overview
total_number_firms = target_df['cusip'].nunique()
total_number_products = target_df['product_id'].nunique()
avg_number_products_firm = total_number_products / total_number_firms

# Save
# target_df.to_csv(r"../Amazon Project - Data/firms_products_link/firms_products_allcolumns.csv")
# target_df.to_excel(r"../Amazon Project - Data/firms_products_link/firms_products_allcolumns.xlsx")

# target_df_skimmed.to_csv(r"../Amazon Project - Data/firms_products_link/firms_products_skimmed.csv")
# target_df_skimmed.to_excel(r"../Amazon Project - Data/firms_products_link/firms_products_skimmed.xlsx")

print("we are done")
