import pandas as pd

more94 = pd.read_excel(r"../Amazon Project - Data/Results/matching_output_fWRatio_morethan94.xlsx")
more90 = pd.read_excel(r"../Amazon Project - Data/Results/matching_output_fratio_morethan90.xlsx")
hardmerge = pd.read_csv(r"../../Amazon Project - Data/Results/metadata_products_hardmerge.csv")
sortratio = pd.read_csv(r"../../Amazon Project - Data/Results/matching_output_fSortratio.csv")

more90 = more90.drop('ratio', axis=1)
more94 = more94.drop('wratio', axis=1)
hardmerge = hardmerge.drop(['Unnamed: 0', 'asin', 'title', 'category', 'date', 'description', 'Unnamed: 0.1',
                            'product_id', 'company_id', 'product_type', 'ticker', 'cusip', 'company name', 'cik',],
                            axis=1)

mask1 = more90.amazon.isin(more94.amazon)
more90_2 = more90[~mask1]

mask2 = hardmerge.brand.isin(more94.amazon)
hardmerge_2 = hardmerge[~mask2]


hardmerge_2 = hardmerge_2.rename(columns={'brand': 'amazon', 'product_name': 'factset', 'main_cat': 'category'})
hardmerge_2 = hardmerge_2[['amazon', 'factset', 'category']]

df_final = more94
df_final = df_final.append(more90_2, ignore_index=True)
df_final = df_final.append(hardmerge_2, ignore_index=True)



filtersort = sortratio.loc[sortratio['sort_ratio'] >= 98]
mask3 = filtersort.amazon.isin(df_final.amazon)
filtersort_2 = filtersort[~mask3]

