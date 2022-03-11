import pandas as pd
import json
import os
import tabula
from fuzzywuzzy import fuzz
import numpy as np


companies_list = pd.read_excel(r"../Amazon Project - Data/Firms List/Public_Companies_Universe_from_edu.xlsx")
matching_results = pd.read_csv(
    r"../../Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching_w_permnos.csv",
    low_memory=False)
matching_factset = pd.read_csv(
    r"../../Amazon Project - Data/firms_products_link/firms_products_skimmed_nodup_permnos.csv")

companies = matching_results['company name'].drop_duplicates().tolist()
comp = companies_list['company_name'].drop_duplicates().tolist()

final_output_function = []
def matching(company_from_matching):

    wratios = np.array(list(map(lambda x: fuzz.WRatio(x, company_from_matching), comp)))

    if np.max(wratios) > 80:
        df = pd.DataFrame({"ciq_companies": comp, "wratio": wratios},
                                 columns=["ciq_companies", "wratio"])
        match_company = df.loc[df.wratio == df.wratio.max(), "ciq_companies"].values[0]

        match_score = df.loc[df.wratio == df.wratio.max(), "wratio"].values[0]

        # We append our results.
        final_output_function.append({"company": company_from_matching, "ciq_companies": match_company, "wratio": match_score})

    else:
        pass


list(map(lambda x: matching(x), companies))
final_output_function = pd.DataFrame(final_output_function, columns=["company", "ciq_companies", "wratio"])
final_output_function.to_excel(r"../Amazon Project - Data/Firms List/firms_match_for_ciqid.xlsx", index=False)