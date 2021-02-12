"""
The following code has the purpose of cleaning a dataset downloaded from Compustat fundamentals annual that contains all
the firms in the US between 2003-01 and 2012-12.
"""

import pandas as pd

comp_na_daily = pd.read_stata(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\Firms list compustat\comp_na_daily_all.dta")

comp_na_daily_nodup = comp_na_daily.drop_duplicates(subset=['conm'])

# drop rows if 'cusip' column is empty
dropped = pd.DataFrame(columns=comp_na_daily_nodup.columns)
CUSIP = 8
i = 0
length_begin = len(comp_na_daily_nodup['cusip'])
for index, row in comp_na_daily_nodup.iterrows():
    roww = row[CUSIP]
    if row[CUSIP] == '':
        i += 1
        dropped.append(pd.Series(dict(zip(dropped.columns, row))), ignore_index=True)
        comp_na_daily_nodup.drop(index, inplace=True)
    else:
        pass
length_final = len(comp_na_daily_nodup['cusip'])

if (length_begin - length_final) == i:
    print("Good job")

comp_na_daily_nodup.to_csv(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\Firms list compustat\list_of_firms_compustat.csv")
dropped.to_excel(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\Firms list compustat\firms_nocusip_compustat.xlsx")

comp_na_daily_nodup.to_excel(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\Firms list compustat\list_of_firms_compustat.xlsx")

print("hello, I'm done")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# ======================================================================================================================
# ======================================================================================================================
#                                          SCRAP CODE REPOSITORY FROM HERE ON
# ======================================================================================================================
# ======================================================================================================================

# dataset = pd.read_stata(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\CIQ_Factset_link\Funda_na_daily_all.dta")
#
# firms_all_time = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Firms List\Firms lists from CIQ\Amazon Project - Firms all time series.xlsx")
#
# target_dataset_right = pd.merge(dataset, firms_all_time, how='right', left_on='cik', right_on='CIK')
#
# target_dataset_inner = pd.merge(dataset, firms_all_time, how='inner', left_on='cik', right_on='CIK')
#
# target_dataset_inner_nodup = target_dataset_inner.drop_duplicates(subset=['cusip'])
# print("hello, I'm almost done")
