import pandas as pd

merged_metadata_products = pd.read_csv(
    r"/Amazon Project - Data/Results/metadata_products_fuzzymatching.csv")

merged_metadata_products_reviews = pd.read_csv(
    r"/Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching.csv",
    low_memory=False)

excel_permnos = pd.read_excel(
    r'D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\firms_products_link\firms_products_skimmed_nodup_permnos.xlsx')


mask1 = excel_permnos.product_name.isin(merged_metadata_products_reviews.factset)
excel_permnos2 = excel_permnos[mask1]
excel_permnos2_nodup = excel_permnos2.drop_duplicates(subset='product_name')
merged_permnos = pd.merge(merged_metadata_products_reviews, excel_permnos2_nodup, how='inner', left_on='factset',
                          right_on='product_name')
clean = merged_permnos.drop(
    columns=['Unnamed: 0', 'date', 'description', 'Unnamed: 0.1', 'amazon', 'category_y', 'category', 'product_id',
             'company_id', 'product_type', 'ticker'])
clean = clean[
    ['LPERMNO', 'LPERMCO', 'asin', 'overall', 'vote', 'reviewTime', 'unixReviewTime', 'title', 'brand', 'category_x',
     'main_cat', 'factset', 'product_name', 'company name', 'cik', 'CUSIP']]

clean.to_csv(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Results\metadata_products_reviews_fuzzymatching_w_permnos.csv",
    index=False)
