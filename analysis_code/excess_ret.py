"""""""""
Code PURPOSE: compute excess returns for Carhart-Fama-French regressions in Table 3 of CKB paper

"""""""""

import pandas as pd


""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# load monthly financial returns for 280 PERMNOs
monthly_returns = pd.read_csv(
    r"../../Amazon Project - Data/support_data/CKB replication/returns_permnos_wratio_5votes.csv")

# Load 3 factors (Fama-French model)
factors_3FF = pd.read_csv(r"../../Amazon Project - Data/support_data/CKB replication/F-F_Research_Data_Factors.csv")

# load fourth factor (Carhart)
factors_MOM = pd.read_csv(r"../../Amazon Project - Data/support_data/CKB replication/F-F_Momentum_Factor.csv")


"""""""""""""""""""""""""""""""""""""""""""""" Start building Frames """""""""""""""""""""""""""""""""""""""""""""""""""
# bring together 'factors_3FF' and 'factors_MOM' to have what's needed for the left-hand side of the regression
factors_4CFF_notscaled = pd.merge(factors_3FF, factors_MOM, on='date', how='inner')
factors_4CFF_notscaled['Mkt-RF'] = factors_4CFF_notscaled['Mkt-RF'] / 100
factors_4CFF_notscaled['SMB'] = factors_4CFF_notscaled['SMB'] / 100
factors_4CFF_notscaled['HML'] = factors_4CFF_notscaled['HML'] / 100
factors_4CFF_notscaled['Mom'] = factors_4CFF_notscaled['Mom'] / 100
factors_4CFF_notscaled['RF'] = factors_4CFF_notscaled['RF'] / 100
factors_4CFF_notscaled.rename(columns={'Mkt-RF': 'Mkt_RF'}, inplace=True)
factors_4CFF = factors_4CFF_notscaled

# extracting the risk-free rates to build excess returns for the PERMNOs
risk_free_rates = factors_4CFF[['date', 'RF']]

# prepare DataFrames and merge 'monthly returns' with 'risk_free_rates' to have PERMNO returns and risk-free rates in
# the same DF
risk_free_rates = risk_free_rates.astype({'date': str})
monthly_returns = monthly_returns.replace({'RET': {'C': 'nan', 'B': 'nan'}})
monthly_returns = monthly_returns.astype({'date': str, 'RET': float})
monthly_returns['date'] = monthly_returns['date'].str[:6] # extract only year and month from the date
returns_with_risk_free = pd.merge(monthly_returns, risk_free_rates, on='date', how='inner').sort_values(
    by=['PERMNO', 'date']).reset_index()
returns_with_risk_free.drop(columns='index',
                            inplace=True)  # dropping 'index' column created when resetting the index in previous line

# build excess returns (XRET) and arrange columns
returns_with_risk_free['XRET'] = returns_with_risk_free['RET'] - returns_with_risk_free['RF']

col_order = ['date', 'PERMNO', 'COMNAM', 'RET', 'RF', 'XRET', 'CUSIP', 'SICCD']
returns_with_risk_free = returns_with_risk_free[col_order]

# save
returns_with_risk_free.to_csv(r"../Amazon Project - Data/support_data/CKB replication/excess_returns_wratio_5votes.csv", index=False)
factors_4CFF.to_csv(r"../Amazon Project - Data/support_data/CKB replication/factors_4CFF.csv", index=False)

print("Code executed")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                        END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

