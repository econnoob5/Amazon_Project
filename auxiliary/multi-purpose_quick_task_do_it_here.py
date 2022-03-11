""""""""""""""""""""""""""""""""""""""""""""""""""" INTRODUCTION """"""""""""""""""""""""""""""""""""""""""""""""""""""
PURPOSE: the following code has been created to avoid .py file proliferation. It will include many of the tasks that do 
         not require extensive coding.
         
USE: Turn on and off the different sections according to the needs. 
     Each section should contain the relevant directories to avoid confusion.

STRUCTURE: This is the most important part. It has to be updated every time a new meaningful section is added, to 
           avoid confusion and to speed up searches for the codes needed.
           - Housekeeping
           - Counting and visualization of output files
           - Excluding from 'Metadata' the products already matched with fuzz.WRatio (20/4/21)
           - Extracting from 'Metadata_small' all the NaN brands
           - Extracting tables from PDF
           - Extracting firms from fuzzy-matching_firms_and_brands (for 10-K scraping)
           - Extracting company names from Edu's results for CIQ scraping (24/6/21)
           - Bring together results from 'parallel1' and 'parallel2' (28/6/21)
           - Working with firms' identification codes
           - aggregating reviews yearly from monthly

""""""""""""""""""""""""""""""""""""""""""""""""" Housekeeping """""""""""""""""""""""""""""""""""""""""""""""""""""""""
############################ Importing packages ############################
import re
import pandas as pd
import json
import os
import tabula
from fuzzywuzzy import fuzz
import numpy as np

# from ctypes.util import find_library
# find_library("gs")
############################ Setting functions of general use ############################

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

# The following function is used to write amd save a json file to a directory
def writeToJSONFile(save_path, fileName, data):
    filePathNameWExt = save_path + '/' + fileName + '.json'
    with open(filePathNameWExt, 'w') as fp:
        json.dump(data, fp, indent=2)


"""""""""""""""""""""""""""""""""""""""" Counting and visualization """""""""""""""""""""""""""""""""""""""""""""""""""

############################################### Setting directories ###################################################
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small"

############################################### Count stuff ###########################################################

# file_imported = pd.read_csv(r"../Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup_permnos.csv")
# lpermno = file_imported.LPERMNO.nunique()
# lpermco = file_imported.LPERMCO.nunique()
# prod_name = file_imported.product_name.nunique()
#
# matching_output = pd.read_csv(r"../Amazon Project - Data/Results/matching_output_final1.csv")

# Counting duplicate products in 'Metadata_small'

# print("-------------------------- duplicates in 'asin' and 'brand' per category --------------------------")
# for file in os.listdir(dir_load):
#     filename = "/" + file
#     df = getDF(dir_load + filename)
#     counting_asin = len(df) - df.asin.nunique()
#     counting_brand = len(df) - df.brand.nunique()
#     print(filename[6:] + " - " + "in asin, " + str(counting_asin) + " | " + "in brand, " + str(counting_brand))

# Counting length of 'Metadata_small_brand_nan'
# dir_load = r"../Amazon Project - Data/Metadata/Metadata_small_brand_nan"
# 
# total_length = 0
# total_duplicates = 0
# for file in os.listdir(dir_load):
#     filename = "/" + file
#     df = getDF(dir_load + filename)
# 
#     try:
#         duplicate_titles = len(df) - df.title.nunique()
# 
#     except AttributeError:
#         print(filename)
# 
#     try:
#         df = df.drop_duplicates(subset='title')
#     except:
#         print('no duplicates')
# 
#     counting_length = len(df)
#     total_length += counting_length
#     total_duplicates += duplicate_titles
#     nodup_titles = total_length - total_duplicates
# 
#     print(str(counting_length) + " " + str(filename))
#
# csv = pd.read_excel(r"D:\Dropbox (IESE)\Amazon Project\2. Data\Old Data\Matches\all_matches.xlsx")
# csv.to_csv(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\all_matches.csv")
#
# rev = pd.read_csv(r"../Amazon Project - Data/metadata_products_reviews_fuzzymatching_fromallmatches.csv")
# load = pd.read_csv(r"../Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching.csv", low_memory=False)



""""""""""""""" Excluding from 'Metadata' the products already matched with fuzz.WRatio (20/4/21) """""""""""""""""""""

# # NOT RUN YET!!!!
# ############################################## Setting directories ###################################################
# matching_output = pd.read_csv(
#     r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results/metadata_products_fuzzymatching.csv")
# dir_load_metadata = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_small-brand_nodup"
# dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Results"
#
# ############################################## Merging ###############################################################
#
# merged_metadata_products = pd.DataFrame()
# for file in os.listdir(dir_load_metadata):
#     filename = "/" + file
#     df = getDF(dir_load_metadata + filename)
#     filter1 = df.brand.isin(matching_output.brand)
#     df = df[~filter1]
#
#     df_merged = pd.merge(df, matching_output, how='inner', left_on='brand', right_on='amazon')
#     merged_metadata_products = merged_metadata_products.append(df_merged, ignore_index=True)
#     print("{} - first loop".format(file))



""""""""""""""""""""""""""" Extracting from 'Metadata_small' all the NaN brands """""""""""""""""""""""""""""""""""""""
# Setting directories
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small"
# dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small_brand_nan"
#
# # Extracting
# i = 0
# # df_final = pd.DataFrame()
# for file in os.listdir(dir_load):
#     filename = "/" + file
#     df = getDF(dir_load + filename)
#
#     df_keep = pd.DataFrame()
#     mask = df.brand.isna()
#     df_keep = df_keep.append(df.loc[mask])
#
#     df_keep.to_json(dir_save + "/{}".format(file), orient='records', lines=True)
#
#     count_nans = df.brand.isna().sum()
#
#     i += 1
#     print('File {}, {}, processed. It has {} columns. it has {} NaNs.'.format(i, file[:-5], len(df_keep),
#                                                                                   str(count_nans)))
#     print("")



""""""""""""""""""""""""""" Extracting from 'Metadata_small' all the NaN brands """""""""""""""""""""""""""""""""""""""

# get_permnos = pd.read_csv(r"../Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching_w_permnos.csv")
# source_permnos = pd.read_csv(r"../Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup_permnos.csv")
# 
# mask = source_permnos.LPERMNO.isin(get_permnos.LPERMNO)
# selected_from_fuzzy_matching = source_permnos[mask]
# 
# duplicates_permno = len(selected_from_fuzzy_matching) - selected_from_fuzzy_matching.LPERMNO.nunique()
# duplicates_permco = len(selected_from_fuzzy_matching) - selected_from_fuzzy_matching.LPERMCO.nunique()
# 
# permnos_nodup = selected_from_fuzzy_matching.drop_duplicates(subset='LPERMNO')
# permnos_nodup = permnos_nodup.sort_values('company name')
# permcos_nodup = selected_from_fuzzy_matching.drop_duplicates(subset='LPERMCO')
# permcos_nodup = permcos_nodup.sort_values('company name')
# 
# # basic statistics
# permnos_fuzzmatch = get_permnos.LPERMNO.nunique()
# permnos_factset = source_permnos.LPERMNO.nunique()
# permnos_selected = selected_from_fuzzy_matching.LPERMNO.nunique()
#
# permnos_nodup.to_excel(r"../Amazon Project - Data/support_data/Data quality check/table3_allcompanies.xlsx", index=False)


""""""""""""""""""""""""""""""""""""""""""" Extracting tables from PDF """""""""""""""""""""""""""""""""""""""""""""""""

# file = r'../Customer Knows Best - Huang (2018) - Online Appendix.pdf'
# tables = tabula.read_pdf(file, pages='3-9', multiple_tables=(True), pandas_options={'header': None})
# df = pd.concat(tables).reset_index(drop=True, inplace=True)
# df.to_excel(r"../Amazon Project - Data/support_data/Data quality check/allfirms_from_CKB.xlsx", index=False)


""""""""""""""""""""""""" Extracting firms from fuzzy-matching_firms_and_brands (for 10-K scraping) """""""""""""""""""""""""""""""""""""

# dir_load = r"../Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching_w_permnos.csv"
#
# fuzz_results = pd.read_csv(dir_load, low_memory=False)
# firms = fuzz_results['company name'].drop_duplicates()
# number_firms = len(firms)


# print('Stop')


""""""""""""""""""""""" Extracting company names from Edu's results for CIQ scraping """""""""""""""""""""""""""""""""""

# companies_list = pd.read_excel(r"../Amazon Project - Data/Firms List/Public_Companies_Universe_from_edu.xlsx")
# matching_results = pd.read_csv(r"../Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching_w_permnos.csv",
#                                low_memory=False)
# matching_factset = pd.read_csv(r"../Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup_permnos.csv")
#
# companies = matching_results['company name'].drop_duplicates().tolist()
# comp = companies_list['company_name'].drop_duplicates().tolist()
#
# final_output_function = []
# def matching_firms_and_brands(company_from_matching):
#
#     wratios = np.array(list(map(lambda x: fuzz.WRatio(x, company_from_matching), comp)))
#
#     if np.max(wratios) > 80:
#         df = pd.DataFrame({"ciq_companies": comp, "wratio": wratios},
#                                  columns=["ciq_companies", "wratio"])
#         match_company = df.loc[df.wratio == df.wratio.max(), "ciq_companies"].values[0]
#
#         match_score = df.loc[df.wratio == df.wratio.max(), "wratio"].values[0]
#
#         # We append our results.
#         final_output_function.append({"company": company_from_matching, "ciq_companies": match_company, "wratio": match_score})
#
#     else:
#         pass
#
#
# list(map(lambda x: matching_firms_and_brands(x), companies))
# final_output_function = pd.DataFrame(final_output_function, columns=["company", "ciq_companies", "wratio"])
# final_output_function.to_excel(r"../Amazon Project - Data/Firms List/firms_match_for_ciqid.xlsx", index=False)
#
# final_output_function = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\firms_match_for_ciqid.xlsx")
# matches_tokenset = []
# matches_ratio = []
# matches_tokensort = []
#
# for row in final_output_function.itertuples():
#     matching_tokenset = fuzz.token_set_ratio(row[1].lower(), row[2].lower())
#     matching_ratio = fuzz.ratio(row[1].lower(), row[2].lower())
#     matching_tokensort = fuzz.token_sort_ratio(row[1].lower(), row[2].lower())
#
#     matches_tokenset.append(matching_tokenset)
#     matches_ratio.append(matching_ratio)
#     matches_tokensort.append(matching_tokensort)
#
#     if matching_tokenset < 90:
#         print(matching_tokenset)
#
# final_output_function['token_set'] = matches_tokenset
# final_output_function['ratio'] = matches_ratio
# final_output_function['token_sort'] = matches_tokensort



""""""""""""""""""""""""""""" Bring together results from 'parallel1' and 'parallel2' """""""""""""""""""""""""""""""""

# results_ratio = pd.read_excel(r'../Amazon Project - Data/Firms List/firms_match_for_ciqid_ratio.xlsx')
# results_set_ratio = pd.read_excel(r'../Amazon Project - Data/Firms List/firms_match_for_ciqid_set_ratio.xlsx')
# results_wratio = pd.read_excel(r'../Amazon Project - Data/Firms List/firms_match_for_ciqid.xlsx')
#
# results_merged = pd.merge(results_ratio, results_set_ratio, how='inner', on='company')
# results_merged = pd.merge(results_merged, results_wratio, how='inner', on='company')
#
# results_merged.to_excel(r'../Amazon Project - Data/Firms List/firms_match_for_ciqid_merged.xlsx', index=False)



""""""""""""""""""""""""""""" Working with firms' identification codes """""""""""""""""""""""""""""""""

############### 15/10/21 ##################
# permnos_alone = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\CKB replication\280_permnos.xlsx")
# permnos_many_columns = pd.read_csv(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\CKB replication\280_permnos_cusip_gvkey_cik.csv", sep=";")
#
# # permnos_many_columns.dropna(subset=['GVKEY'])
# permnos_many_columns.astype({'GVKEY': 'int64', 'LPERMNO': 'int64', 'LPERMCO': 'int64'})
#
# permnos = permnos_many_columns.LPERMNO.drop_duplicates()
# gvkey = permnos_many_columns.GVKEY.drop_duplicates()
# cusip = permnos_many_columns.CUSIP.drop_duplicates()
#
# permnos_many_cols_nodup = permnos_many_columns.drop_duplicates(subset=['LPERMNO'])
#
# np.savetxt(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\CKB replication\gvkey_from_280_permnos.txt", gvkey, fmt='%d')
#
#
# xlsx_ticker = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\CKB replication\capx_r&d_280permnos.xlsx")
#
# xlsx_ticker['Ticker Symbol'] = xlsx_ticker['Ticker Symbol'].astype(str)
# ticker = xlsx_ticker['Ticker Symbol'].drop_duplicates()
#
# np.savetxt(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\CKB replication\tic_from_280_permnos.txt", ticker, fmt='%s')
#
# stata = pd.read_stata(r"/Users/mattia/Documents/Mattia/Get Busy/Jobs/Ruben/classif_check.dta")


""""""""""""""""""""""""""""" aggregating reviews yearly from monthly """""""""""""""""""""""""""""

df_5_rev_monthly = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\permnos_wratio_5votes.xlsx")
df_fin_data = pd.read_stata(r"/Amazon Project - Data/Financial Data/CrspCompMerge_allvars.dta")
df_49FF_ind = pd.read_fwf(r"/Amazon Project - Data/Financial Data/FF49_Siccodes.txt")

df_5_rev_monthly_transposed = df_5_rev_monthly.transpose()
df_5_rev_monthly_transposed['Index'] = df_5_rev_monthly_transposed.index
cols = df_5_rev_monthly_transposed.columns.tolist()
cols = cols[-1:] + cols[:-1]
df_5_rev_monthly_transposed = df_5_rev_monthly_transposed[cols]

l_index = df_5_rev_monthly_transposed['Index'].tolist()
list_new_index = [str[:-2] for str in l_index]
list_new_index = [string.replace('LPERM', 'LPERMNO') for string in list_new_index]

df_5_rev_monthly_transposed['Index'] = list_new_index
df_5_rev_monthly_transposed.columns = df_5_rev_monthly_transposed.iloc[0]
df_5_rev_monthly_transposed.drop(df_5_rev_monthly_transposed.index[0], inplace=True)

df_yearly_rev = df_5_rev_monthly_transposed.groupby('LPERMNO').sum()
df_yearly_rev_transposed = df_yearly_rev.transpose()

for row in df_yearly_rev.itertuples():
    list_values = list(row)

    # drop year in index
    del list_values[0]
    # sort increasingly
    list_values.sort()

df_fin_data["LPERMNO"] = df_fin_data["LPERMNO"].astype(np.int64)
list_permnos = df_yearly_rev.columns.tolist()

mask = df_fin_data.LPERMNO.isin(list_permnos)
df_fin_data2 = df_fin_data[mask]


df_49FF_ind = df_49FF_ind.shift(periods=1, fill_value=0)
cols_49 = df_49FF_ind.columns.to_list()
df_49FF_ind.iloc[0] = cols_49
df_49FF_ind.rename(columns={"1": "FF49", "Agric": "Category", "Agriculture": "SIC"}, inplace=True)

list_ff49col = df_49FF_ind['FF49'].tolist()

list_new_ff49col = [int(element) if str(element) != 'nan' else element for element in list_ff49col]

list_new_dfcol = []
for el in list_new_ff49col:
    if isinstance(el, int):
        list_new_dfcol.append(el)
    
    else:
        last_elem = list_new_dfcol[-1]
        list_new_dfcol.append(last_elem)

df_49FF_ind['FF49'] = list_new_dfcol

l_ff49_sic = []
for row in df_49FF_ind.itertuples():
    s_ff49 = str(row[1])

    s_sic = str(row[3])
    s_sic = s_sic[:9]

    l_temp = []
    # keep only if number and dash are present
    if re.search(r'[(\d+)(-)]', s_sic):
        first_elem = s_sic[:4]
        second_elem = s_sic[5:9]
        l_temp.extend((s_ff49, first_elem, second_elem))

    else:
        continue

    # append is list non-empty
    if l_temp:
        l_ff49_sic.append(l_temp)


l_sic_fromfindata = df_fin_data2['sic'].tolist()
l_ff49 = []
l_norange_layer2 = []
l_layer1 = []

for s1 in l_sic_fromfindata:
    # get sic in big dataset
    s_sic = int(s1)
    l_temp = []
    l_temp_norange = []
    
    for s2 in l_ff49_sic:
        # construct sic ranges to select appropriate FF49 code
        rangeLow = int(s2[1])
        rangeHigh = int(s2[2])

        if rangeLow <= s_sic <= rangeHigh:
            # get FF49 code
            s_ff49_code = int(s2[0])
            l_temp.append(s_ff49_code)
        else:
            l_temp_norange.append(s_sic)
            
    if l_temp:
        l_ff49.append(int(l_temp[0]))
    else:
        s_nan = 'nan'
        l_ff49.append(float(s_nan))
        l_layer1.append(s_sic)

# add FF49 codes to big dataset next to sic column
df_fin_data2.insert(974, 'FInd', l_ff49)
df_fin_data2 = df_fin_data2.astype({'GVKEY': 'int64', 'sic': 'int64'})
df_fin_data2.dropna(subset=['FInd'], inplace=True)

# df_fin_data2.to_stata(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Financial Data\CrspCompMerge_403permno.dta", version=117)