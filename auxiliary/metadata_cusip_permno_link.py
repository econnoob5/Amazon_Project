"""""""""
Code PURPOSE: companies in 'metadata_products_reviews_hardmerge.csv' do not have PERMNO codes. We retrieved such codes
              from CRSP/Compustat merge table. The following code performs the merging between our data and the
              companies with PERMNO codes.
              
Code OUTPUT: 'metadata_products_reviews_hardmerge.csv' with PERMNO codes
"""""""""

import pandas as pd
import os

""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + "\\"
TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Results/metadata_products_reviews_hardmerge.csv"
AUXILIARY_FOLDER_ROOT_DIR = r"Amazon Project - Data/support_data/Miscellaneous/cusip_permno_link_metadata.csv"
SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data\Results"
dir_load_target = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
dir_load_auxiliary = os.path.join(PROJECT_ROOT_DIR, AUXILIARY_FOLDER_ROOT_DIR)
dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)


"""""""""""""""""""""""""""""""""""""""""""""""""""" Merging """""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# load files
df_target = pd.read_csv(dir_load_target)
df_target = df_target.fillna(000)
df_target.cik = df_target.cik.astype(int)

df_auxiliary = pd.read_csv(dir_load_auxiliary)

# perform some prior checks on column lengths to prepare inspection of data loss
check_length_before_cusip = len(df_target['cusip'])
check_unique_before_cusip = df_target['cusip'].nunique()
print("Cusip length is: {}, while unique cusips are: {}".format(check_length_before_cusip, check_unique_before_cusip))
check_length_before_cik = len(df_target['cik'])
check_unique_before_cik = df_target['cik'].nunique()
print("Cik length is: {}, while unique cik are: {}".format(check_length_before_cik, check_unique_before_cik))
check_length_before_brand = len(df_target['brand'])
check_unique_before_brand = df_target['brand'].nunique()
print("Brand length is: {}, while unique brands are: {}".format(check_length_before_brand, check_unique_before_brand))
check_length_before_companyname = len(df_target['company name'])
check_unique_before_companyname = df_target['company name'].nunique()
print("Company_name length is: {}, while unique company_names are: {}".format(check_length_before_companyname,
                                                                              check_unique_before_companyname))

print("")

# merge
df_merged_cusip = pd.merge(df_target, df_auxiliary, how='inner', left_on='cusip', right_on='CUSIP')
df_merged_cik = pd.merge(df_target, df_auxiliary, how='inner', on='cik')
print("******************** merge ***********************")
print("")

# perform some posterior checks on column lengths to inspect potential data loss
check_length_after_cusip = len(df_merged_cusip['cusip'])
check_unique_after_cusip = df_merged_cusip['cusip'].nunique()
check_unique_after_CUSIP = df_merged_cusip['CUSIP'].nunique()
print("Cusip length is: {}, while unique cusips are: {}, while CUSIP are: {}".format(check_length_after_cusip,
                                                                                     check_unique_after_cusip,
                                                                                     check_unique_after_CUSIP))
check_length_after_cusip_brand = len(df_merged_cusip['brand'])
check_unique_after_cusip_brand = df_merged_cusip['brand'].nunique()
print("Cusip_brand length is: {}, while unique cusip_brands are: {}".format(check_length_after_cusip_brand,
                                                                            check_unique_after_cusip_brand))
check_length_after_cusip_companyname = len(df_merged_cusip['company name'])
check_unique_after_cusip_companyname = df_merged_cusip['company name'].nunique()
print("Cusip_company_name length is: {}, while unique cusip_company_name are: {}".format(
    check_length_after_cusip_companyname, check_unique_after_cusip_companyname))

print("")

check_length_after_cik = len(df_merged_cik['cik'])
check_unique_after_cik = df_merged_cik['cik'].nunique()
print("Cik length is: {}, while unique cik are: {}".format(check_length_after_cik, check_unique_after_cik))
check_length_after_cik_brand = len(df_merged_cik['brand'])
check_unique_after_cik_brand = df_merged_cik['brand'].nunique()
print("Cik_brand length is: {}, while unique cik_brand are: {}".format(check_length_after_cik_brand,
                                                                       check_unique_after_cik_brand))
check_length_after_cik_companyname = len(df_merged_cik['company name'])
check_unique_after_cik_companyname = df_merged_cik['company name'].nunique()
print("Cik_company_name length is: {}, while unique cik_company_name are: {}".format(check_length_after_cik_companyname,
                                                                                     check_unique_after_cik_companyname))

permnos_count = df_merged_cik['LPERMNO'].nunique()

# Saving output from the cik merge (there is no difference between the two)
df_merged_cik = df_merged_cik.drop(
    ['Unnamed: 0', 'date', 'Unnamed: 0.1', 'CUSIP', 'conm', 'LINKPRIM', 'LIID', 'LINKTYPE', 'LINKDT', 'LINKENDDT',
     'product_type', 'product_name', 'ticker', 'description', 'product_id', 'company_id'], axis=1)
column_list = ['asin', 'LPERMNO', 'company name', 'brand', 'main_cat', 'overall', 'vote', 'reviewTime',
               'unixReviewTime', 'title', 'category', 'cusip', 'cik', 'gvkey', 'LPERMCO']
df_merged_cik = df_merged_cik.reindex([i for i in column_list if i in df_merged_cik.columns], axis=1)
df_merged_cik.to_csv(dir_save + "/metadata_products_reviews_hardmerge_w_permno.csv", index=False)
df_merged_cik.to_excel(dir_save + "/metadata_products_reviews_hardmerge_w_permno.xlsx", index=False)

print("Code executed")
