"""
The purpose of the following code is to merge the firms coming from compustat fundamentals annual, and which list was
cleaned in the code 'firms_list_compustat_cleaning.py', with their corresponding products, coming from Factset Revere -
Products.
"""

import pandas as pd

# load list of products from factset
products = pd.read_csv(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Products Lists\factset_all_products.csv")

# load list of firms from compustat
firms = pd.read_csv(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\Firms list compustat\list_of_firms_compustat.csv")

# merge the factset and compustat together
target_df = pd.merge(products, firms, how='inner', on='cusip').rename(columns={'conm':'company name'})

target_df_skimmed = target_df.drop(columns=['start_', 'end_', 'name', 'active', 'isin', 'Unnamed: 0', 'gvkey', 'datadate',
                                   'fyear', 'indfmt', 'consol', 'popsrc', 'datafmt', 'tic', 'curcd', 'costat'], axis=1)

# compute two simple statistics for general overview
average_number_of_products_per_firm = len(target_df['cusip']) / len(firms['cusip'])
total_number_firms = target_df['cusip'].nunique()

target_df.to_csv(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\firms_products_link\firms_products_allcolumns.csv")

target_df.to_excel(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\firms_products_link\firms_products_allcolumns.xlsx")

target_df_skimmed.to_csv( r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\firms_products_link\firms_products_skimmed.csv")
target_df_skimmed.to_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\firms_products_link\firms_products_skimmed.xlsx")

print("we are here")
