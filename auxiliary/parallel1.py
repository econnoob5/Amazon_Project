import pandas as pd
import json
import os
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


def matching(company_from_matching, companies):

    ratios = np.array(list(map(lambda x: fuzz.ratio(x, company_from_matching), comp)))

    if np.max(ratios) > 30:
        df = pd.DataFrame({"ciq_companies_ratio": comp, "ratio": ratios},
                          columns=["ciq_companies_ratio", "ratio"])
        match_company = df.loc[df.ratio == df.ratio.max(), "ciq_companies_ratio"].values[0]

        match_score = df.loc[df.ratio == df.ratio.max(), "ratio"].values[0]

        # This simply computes the progress of the loop
        # progress = round(np.where(companies == company_from_matching) / len(companies) * 100, 2)

        # We append our results.
        final_output_function.append({"company": company_from_matching, "ciq_companies_ratio": match_company, "ratio": match_score})

        # print(progress)
        print(df)

    else:
        pass


list(map(lambda x: matching(x, companies), companies))
final_output_function = pd.DataFrame(final_output_function, columns=["company", "ciq_companies_ratio", "ratio"])
final_output_function.to_excel(r"../Amazon Project - Data/Firms List/firms_match_for_ciqid_ratio.xlsx", index=False)