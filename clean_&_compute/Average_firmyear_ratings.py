""""
Code PURPOSE: compute RHS variables for regressions

Code OUTPUT:

Code MECHANICS:
"""""

import pandas as pd
import numpy as np
import math

df_AvgRatings_mm_firms = pd.read_excel(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\avgratings_monthly_5votes_wratio.xlsx")

df_NumRatings_mm_firms = pd.read_excel(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\permnos_wratio_5votes.xlsx")

df_CrspComp_merged = pd.read_stata(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Financial Data\CrspCompMerge_403permno.dta")

# get FF49 from crsp and and add to avg ratings dataset
df_CrspComp_merged = df_CrspComp_merged.drop_duplicates(subset=['LPERMNO'])
mask = df_CrspComp_merged.LPERMNO.isin(df_AvgRatings_mm_firms.LPERMNO)
df_get_ff49 = df_CrspComp_merged[mask]

df_lpermno_ff49_fromCRSP = df_get_ff49[['LPERMNO', 'FInd']]
df_lpermno_fromAvgratings = df_AvgRatings_mm_firms[['LPERMNO']]

df_lpermno_fromAvgratings_withff49 = pd.merge(df_lpermno_fromAvgratings, df_lpermno_ff49_fromCRSP, on='LPERMNO',
                                              how='left')

df_AvgRatings_mm_firms.insert(1, 'FInd', df_lpermno_fromAvgratings_withff49['FInd'])

# df that at first hosts avg ratings per year per permno and then also contains the industry averages
df_AvgRat_yy = pd.DataFrame()
# df with number of ratings per year per permno
df_TotNumRat_yy = pd.DataFrame()
# df with yearly industry averages
df_IndRat_yy = pd.DataFrame()
# df with firm level delta from the industry mean in each year mean
df_D_from_IndAvg_yy = pd.DataFrame()
# df with industry std deviation
df_IndStdDev_yy = pd.DataFrame()
for y1 in range(1999, 2019):
    s_year1 = str(y1)
    # select rating in the same year
    df_Filter_AvgRat_mm = df_AvgRatings_mm_firms.filter(like=s_year1, axis='columns')
    df_Filter_NumRat = df_NumRatings_mm_firms.filter(like=s_year1, axis='columns')

    # create column names for DataFrames
    s_colname_avgr = 'AvgRating_{}'.format(s_year1)
    s_colname_numr = 'NumRating_{}'.format(s_year1)
    s_colname_indavgr = 'Ind_Avg_Rating_{}'.format(s_year1)
    s_colname_delta = 'delta_from_IndAvg_{}'.format(s_year1)
    s_colname_indstddev = 'Ind_stddev_{}'.format(s_year1)

    # total number of ratings per permno in one year
    pds_TotRat = pd.Series(list(df_Filter_NumRat.sum(axis=1)), name=s_colname_numr)
    df_TotNumRat_yy = pd.concat((df_TotNumRat_yy, pds_TotRat.rename(s_colname_numr)), axis=1)

    ### avg rating per permno per year ###
    # construct weights for weighted sum of monthly ratings per permno
    df_Weights = df_Filter_NumRat.div(pds_TotRat, axis=0)
    # multiply by the weights and sum to construct average and concat to final DataFrame
    df_Weighted_MonthlyAvgRat = df_Filter_AvgRat_mm.mul(df_Weights)
    pds_Weighted_YearlyAvgRat = df_Weighted_MonthlyAvgRat.sum(axis=1)
    df_AvgRat_yy = pd.concat((df_AvgRat_yy, pds_Weighted_YearlyAvgRat.rename(s_colname_avgr)), axis=1)

    ### avg rating by industry per year ###
    # get permnos and industry codes
    pds_permno_temp = pd.Series(list(df_AvgRatings_mm_firms.LPERMNO), name='LPERMNO')
    pds_ff49_temp = pd.Series(list(df_AvgRatings_mm_firms.FInd), name='FInd')
    # join the previous two series into a df with also the number of ratings per permno in one year
    df_NumRat_Ind_temp = pd.concat([pds_permno_temp, pds_ff49_temp, pds_TotRat], axis=1)
    # sum the number of ratings received during the year by each industry. Then map the total number to each permno
    df_NumRat_SumInd_temp = df_NumRat_Ind_temp.groupby('FInd', as_index=False)[s_colname_numr].sum()
    df_NumRat = pd.merge(df_NumRat_Ind_temp, df_NumRat_SumInd_temp, how='left', on='FInd')
    # weights (permno ratings/total industry ratings)
    df_NumRat['Weight_AvgInd'] = df_NumRat.iloc[:, 2] / df_NumRat.iloc[:, 3]
    df_NumRat = pd.concat((df_NumRat, pds_Weighted_YearlyAvgRat.rename(s_colname_avgr)), axis=1)
    # construct weighted rating and sum to get industry rating
    df_NumRat['Weighted_Rating_for_IndAvg'] = df_NumRat['Weight_AvgInd'] * df_NumRat[s_colname_avgr]
    df_IndAvg_short = df_NumRat.groupby('FInd', as_index=False)['Weighted_Rating_for_IndAvg'].sum()

    df_IndAvg_short.rename(columns={'Weighted_Rating_for_IndAvg': s_colname_indavgr}, inplace=True)
    df_IndAvg_long = pd.merge(df_NumRat, df_IndAvg_short, how='left', on='FInd')

    df_IndRat_yy = pd.concat((df_IndRat_yy, df_IndAvg_long[s_colname_indavgr]), axis=1)

    ### industry std deviation per year ###
    # construct firm delta from the mean
    df_IndAvg_long[s_colname_delta] = df_IndAvg_long[s_colname_avgr] - df_IndAvg_long[s_colname_indavgr]
    df_D_from_IndAvg_yy = pd.concat((df_D_from_IndAvg_yy, df_IndAvg_long[s_colname_delta]), axis=1)

    df_IndAvg_long['pow2_delta_from_IndAvg'] = df_IndAvg_long[s_colname_delta] ** 2
    l_industries = df_IndAvg_long['FInd'].to_list()

    l_temp_findcode = []
    l_temp_stddev = []
    for i in l_industries:
        fl_temp_indcode = i
        df_temp_StdDev1 = df_IndAvg_long.loc[df_IndAvg_long['FInd'] == i]
        # drop rows if there are no ratings in the year, so the value in col 'Weighted_Rating_for_IndAvg' == 0
        df_temp_StdDev2 = df_temp_StdDev1[~df_temp_StdDev1.eq(0).any(axis='columns')]

        # measures for the std dev
        i_ddof = len(df_temp_StdDev2) - 1
        fl_sumdev = df_temp_StdDev2['pow2_delta_from_IndAvg'].sum()
        fl_var = fl_sumdev / i_ddof
        fl_stddev_indrate = math.sqrt(fl_var)
        l_temp_findcode.append(fl_temp_indcode)
        l_temp_stddev.append(fl_stddev_indrate)

    df_temp_IndStdDev = pd.DataFrame(list(zip(l_temp_findcode, l_temp_stddev)))
    df_temp_IndStdDev.columns = ['FInd', s_colname_indstddev]
    df_IndStdDev_yy = pd.concat((df_IndStdDev_yy, df_temp_IndStdDev[s_colname_indstddev]), axis=1)

# remove the few infinite values resulting from computing the variance
df_IndStdDev_yy.replace([np.inf, -np.inf], np.nan, inplace=True)

# add permnos and FF49 industry codes
df_AvgRat_yy.insert(0, 'LPERMNO', df_AvgRatings_mm_firms['LPERMNO'])
df_AvgRat_yy.insert(1, 'FInd', df_AvgRatings_mm_firms['FInd'])
df_TotNumRat_yy.insert(0, 'LPERMNO', df_AvgRatings_mm_firms['LPERMNO'])
df_TotNumRat_yy.insert(1, 'FInd', df_AvgRatings_mm_firms['FInd'])

df_AllMeasures_v1 = pd.concat((df_AvgRat_yy, df_IndRat_yy, df_D_from_IndAvg_yy, df_IndStdDev_yy), axis=1)

# df with firm level standardized delta from the industry mean in each year mean
df_StdDelta_from_IndAvg_yy = pd.DataFrame()
for y2 in range(1999, 2019):
    s_year2 = str(y2)
    # create column names for DataFrames
    s_colname_delta = 'delta_from_IndAvg_{}'.format(s_year2)
    s_colname_indstddev = 'Ind_stddev_{}'.format(s_year2)
    s_colname_stddelta = 'StdDelta_{}'.format(s_year2)
    # select rating in the same year
    df_temp_AllMeasures = df_AllMeasures_v1.filter(like=s_year2, axis='columns')
    pds_temp_stddelta = df_temp_AllMeasures[s_colname_delta] / df_temp_AllMeasures[s_colname_indstddev]
    # if present replace inf values generated because the std dev (denominator) == 0
    try:
        pds_temp_stddelta.replace([np.inf, -np.inf], np.nan,inplace=True)
    except:
        print('No inf values')

    df_StdDelta_from_IndAvg_yy = pd.concat((df_StdDelta_from_IndAvg_yy, pds_temp_stddelta.rename(s_colname_stddelta)),
                                           axis=1)

df_AllMeasures_v2 = pd.concat((df_AllMeasures_v1, df_StdDelta_from_IndAvg_yy), axis=1)

### construct final file that agrees with the panel structure declared on STATA with xtset ###
# get the years to create the column fyear to use for merging with the permnos on STATA
l_temp = list(df_AllMeasures_v2.columns)
del l_temp[:82]
l_years = [x[-4:] for x in l_temp]
pds_years = pd.Series(l_years, name='fyear')
pds_years = pd.to_numeric(pds_years)

# initialize final file
df_STATA = pd.DataFrame()
for row in df_AllMeasures_v2.itertuples():
    permno = row[1]
    FInd = row[2]

    pds_firm_averages = pd.Series(np.array(row[3:23]), name='Firm_rating_avg')
    pds_ind_averages = pd.Series(np.array(row[23:43]), name='Ind_rating_avg')
    pds_delta_from_ind_averages = pd.Series(np.array(row[43:63]), name='Delta_from_ind_avg')
    pds_ind_stddev = pd.Series(np.array(row[63:83]), name='Ind_stddev')
    pds_stddelta = pd.Series(np.array(row[83:102]), name='Std_delta')

    pds_permnos = pd.Series(np.repeat(permno, len(pds_firm_averages)), name='LPERMNO')
    pds_find = pd.Series(np.repeat(FInd, len(pds_firm_averages)), name='FInd')

    l_pds_toconcat = [pds_permnos, pds_find, pds_years, pds_firm_averages, pds_ind_averages,
                      pds_delta_from_ind_averages, pds_ind_stddev, pds_stddelta]
    df_temp = pd.concat(l_pds_toconcat, axis=1)
    df_STATA = pd.concat((df_STATA, df_temp), ignore_index=True)

df_STATA.to_stata(
    r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Financial Data\for_merging_v2.dta",
    version=117, write_index=False)
