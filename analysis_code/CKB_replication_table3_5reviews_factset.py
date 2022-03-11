"""
Code PURPOSE: replicating table 3 in the Customer Knows Best paper

Code OUTPUT:

Code MECHANICS:
"""

import os
import pandas as pd
from itertools import islice
from collections.abc import Iterable

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                        TO DO BEFORE RUNNING THE CODE

- remember that after first big loop there is a 'load' statement for the 'csv_target_file' file that has to be turned on or off
  according to the needs
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
PURPOSE: - bring together the firms-brands in "all_matches.xlsx" and the brands-reviews from 
           "Metadata" and "Reviews"
         - compute the tercile portfolios for replication of table 3 in CKB 
STRUCTURE: - it is divided into sections delimited by """""""
           - each section executes a main task and contains detailed comment regarding the functioning
             and rationale of the code
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
# TARGET_FILE_ROOT_DIR = r"Amazon Project - Data\Results\metadata_products_reviews_fuzzymatching_w_permnos.csv"
# SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data\Results"
dir_load = r"../../Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching_w_permnos.csv"

csv_target_file = pd.read_csv(
    r"/Amazon Project - Data/Results/metadata_products_reviews_fuzzymatching_w_permnos.csv",
    low_memory=False)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 2: The following piece of code computes the ratings for each permno and each month of available data.
             It also saves the number of reviews per permno and month.
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# 'permnos' is a list of all the permnos that have reviews, found in 'csv_target_file'
permnos = list(csv_target_file['LPERMNO'].unique())
permnos_count = csv_target_file['LPERMNO'].nunique()

#   The code below is to order the year_month in ascending order, which hopefully will come in handy later on.
months = []
m = []
y = []
# this loop saves all year_month of available data, in ascending order.
for entry in list(csv_target_file['reviewTime'].unique()):
    # date takes the month and the year from US format dates
    date = entry[:2] + '/' + entry[-4:]
    if date not in months:
        months.append(date)
        m.append(date.split('/')[0])
        y.append(date.split('/')[1])
y, m = (list(t) for t in zip(*sorted(zip(y, m))))

# here year_month is overwritten to append all the mm/yyyy couples
year_month = []
for i in range(len(y)):  # length is each year x 12
    year_month.append(y[i] + m[i])

# The following two DataFrames will contain:
#     - average_monthly_rating, the average rating of a permno for each month
#     - number_reviews, the number of reviews for that brand on that month

col_start = ['LPERMNO']
average_monthly_rating = pd.DataFrame(columns=col_start + year_month)
number_reviews = pd.DataFrame(columns=col_start + year_month)
#
# # ======================================================================================================================
# # the following for-loop uses 'permnos', which is the list of permnos built before from all the permnos for which we
# # have reviews. For each permno it saves the ratings (Overall), standard time of the review (reviewTime) and Unix time
# # of review (unixReviewTime).
# # The intermediate output is:
# #   - 'reviews', a temporary dataset that has for each permno, the rankings, date and UNIX date from each review
# #   - 'temp_average', a temporary dataset that has for each permno the average reviews in each month,
# #                     if that month there are no reviews print a 'nan'
# #   - 'temp_number', a temporary dataset that has for each permno the number (count) of reviews in each month
# # the final output is:
# #   - 'average_monthly_rating', is a dataset containing the average rating for each permno in each month of the sample
# #   - 'number_reviews', is a dataset containing the number of ratings (therefore also the reviews, although we do not
# #                       have reviews here) for each permno in each month of the sample.
# # ======================================================================================================================
#
# for permno in permnos:
#     reviews = pd.DataFrame(columns=('Overall', 'reviewTime', 'unixReviewTime'))
#     # 'row[1]' is the permno from 'csv_target_file'
#     for row in csv_target_file.itertuples():
#         if row[1] == permno:
#             a = []
#             b = []
#             c = []
#             a.append(row[4])  # get Overall (rating to the product)
#             b.append(row[6])  # get reviewTime
#             c.append(row[7])  # get unixReviewTime
#             prov = pd.DataFrame(list(zip(a, b, c)), columns=('Overall', 'reviewTime', 'unixReviewTime'))
#             reviews = reviews.append(prov, ignore_index=True)
#
#     temp_average = [permno]
#     temp_number = [permno]
#
#     # the following two for-loops take for each date in 'year_month' the corresponding date in 'reviews', constructed
#     # just above, count the number of reviews received in that month, if any, compute the total sum of the reviews and
#     # perform the average rating in that month
#     for month in year_month:  # year_month contains yyyymm
#         number_monthly_reviews = 0  # 'number_monthly_reviews' keeps track of number of reviews in each month
#         total = 0  # 'total' sums up tha ratings (using row[1]) for each brand in each month
#         # row2[2] is the column with review dates in US format
#         for row2 in reviews.itertuples():
#             # date gets the year and the month from 'reviews'
#             date = row2[2][-4:] + row2[2][:2]
#             if date == month:
#                 number_monthly_reviews += 1
#                 total += float(row2[1])
#
#         if number_monthly_reviews != 0:
#             temp_average.append(total / number_monthly_reviews)
#         else:
#             temp_average.append('nan')
#         temp_number.append(number_monthly_reviews)
#
#     prov1 = pd.Series(temp_average, index=average_monthly_rating.columns)
#     average_monthly_rating = average_monthly_rating.append(prov1, ignore_index=True)
#     prov2 = pd.Series(temp_number, index=number_reviews.columns)
#     number_reviews = number_reviews.append(prov2, ignore_index=True)
#     print(permno)

# average_monthly_rating.to_excel(r"../Amazon Project - Data/support_data/Miscellaneous/average_overall_wratio.xlsx", index=False)
# number_reviews.to_excel(r"../Amazon Project - Data/support_data/Miscellaneous/number_reviews_wratio.xlsx", index=False)

# The following for-loops produce two dataframes to check hwo many permnos have at least one month with at least 5 or
# 10 reviews, respectively.
average_monthly_rating = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\average_overall_wratio.xlsx")
number_reviews = pd.read_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\number_reviews_wratio.xlsx")

number_reviews_atleast_5 = pd.DataFrame()
i = -1
for row in number_reviews.itertuples():
    i += 1
    permno_reviews = []
    for element in row[2:]:
        if len(permno_reviews) == 0 and element >= 5:
            permno_reviews.append(row[1])
        elif element >= 5:
            permno_reviews.append(element)
        else:
            pass
    if len(permno_reviews) != 0:
        temp_list = pd.Series(permno_reviews)
        number_reviews_atleast_5 = number_reviews_atleast_5.append(temp_list, ignore_index=True)
number_reviews_atleast_5 = number_reviews_atleast_5.rename(columns={number_reviews_atleast_5.columns[0]:"LPERMNO"})

number_reviews_atleast_10 = pd.DataFrame()
i = -1
for row in number_reviews.itertuples():
    i += 1
    permno_reviews = []
    for element in row[2:]:
        if len(permno_reviews) == 0 and element >= 10:
            permno_reviews.append(row[1])
        elif element >= 10:
            permno_reviews.append(element)
        else:
            pass
    if len(permno_reviews) != 0:
        temp_list = pd.Series(permno_reviews)
        number_reviews_atleast_10 = number_reviews_atleast_10.append(temp_list, ignore_index=True)

number_reviews_atleast_10 = number_reviews_atleast_10.rename(columns={number_reviews_atleast_10.columns[0]:"LPERMNO"})

# apply a filter to 'average_monthly_rating' and 'number_reviews' to eliminate the PERMNOs that do not have at least
# 5 or 10 reviews in at least one month
mask5 = average_monthly_rating.LPERMNO.isin(number_reviews_atleast_5.LPERMNO)
average_overall_merged_5 = average_monthly_rating[mask5].reset_index()
number_reviews_merged_5 = number_reviews[mask5].reset_index()
average_overall_merged_5 = average_overall_merged_5.drop(columns='index')
number_reviews_merged_5 = number_reviews_merged_5.drop(columns='index')


mask10 = average_monthly_rating.LPERMNO.isin(number_reviews_atleast_10.LPERMNO)
average_overall_merged_10 = average_monthly_rating[mask10].reset_index()
number_reviews_merged_10 = number_reviews[mask10].reset_index()
average_overall_merged_10 = average_overall_merged_10.drop(columns='index')
number_reviews_merged_10 = number_reviews_merged_10.drop(columns='index')

# save the two DataFrames to get the PERMNOs to retrieve the returns from Compustat
# number_reviews_merged_5.to_excel(r"..\Amazon Project - Data\support_data\Miscellaneous\permnos_wratio_5votes.xlsx", index=False)
# number_reviews_merged_10.to_excel(r"..\Amazon Project - Data\support_data\Miscellaneous\permnos_wratio_10votes.xlsx", index=False)
average_overall_merged_5.to_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\avgratings_monthly_5votes_wratio.xlsx", index=False)
average_overall_merged_10.to_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\Miscellaneous\avgratings_monthly_10votes_wratio.xlsx", index=False)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 3: At this point we should have two blocks of DataFrames
             -  'average_overall_merged_5', with the monthly average ratings for each permno with at least 5 reviews
                                            in at least on month
             -  'number_reviews_merged_5', with the count of review per month for ALL permnos with at least 5 reviews
                                           in at least on month
             -  'average_overall_merged_10', with the monthly average ratings for each permno with at least 5 reviews
                                             in at least on month
             -  'number_reviews_merged_10', with the count of review per month for ALL permnos with at least 5 reviews
                                            in at least on month
             With this information about the permnos we can generate the 'abnormal ratings' measure

IMPORTANT: remeber to change input in for-loop when running code for different minimum monthly reviews.
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# the following for-loop gives as final output:
#   - 'abnormal_matrix', which includes the abnormal rating for each permno and month
#                       (i.e. the deviation in a month from the average rating in the previous 12 months).
#                       the first column is the permno and following columns are the dates.

abnormal_matrix = pd.DataFrame(
    columns=col_start + year_month[12:])  # Here we assume we'll have observations for all year_month.
                                          # year_month[12:] creates a delay to consider from the 13th month onward
row_index = 0 #TODO: delete after debugging of this forloop
for row3 in average_overall_merged_5.itertuples():
    row_index += 1 #TODO: delete after debugging of this forloop
    stop_it = row_index #TODO: delete after debugging of this forloop
    # row[1] is the permno from average_monthly_rating
    abnormal = [row3[1]]
    # row[14:] takes the length from the 13th month onwards until the end of the sample period
    len_for_deb = len(row3[14:]) #TODO: delete after debugging of this forloop
    for i in range(len(row3[14:])):
        grand_total = 0
        number_rate = 0
        for x in range(1, 13):  # IMPORTANT for indexing: .columns does not count the index column.
            # i carries time forward and x takes care of the lag
            # 'grand_total', computes for each permno the number of ratings in a month * average rating in that month
            # 'number_rate', computes for each permno the number of ratings in a month
            if row3[14 + i - x] != 'nan':
                grand_total += number_reviews_merged_5[average_overall_merged_5.columns[13 + i - x]][
                                   list(number_reviews_merged_5['LPERMNO']).index(row3[1])] * row3[14 + i - x]
                number_rate += number_reviews_merged_5[average_overall_merged_5.columns[13 + i - x]][
                    list(number_reviews_merged_5['LPERMNO']).index(row3[1])]
        if number_rate != 0:
            average = grand_total / number_rate
        else:
            average = 0
        if row3[14 + i] == 'nan':
            abnormal.append('nan')
        else:
            abnormal.append(row3[14 + i] - average)

    prov = pd.Series(abnormal, index=abnormal_matrix.columns)
    abnormal_matrix = abnormal_matrix.append(prov, ignore_index=True)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 4: Now, we compute the tercile portfolios based on abnormal ratings
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# 'final_portfolio', keeps track, each month, of the permnos that are in each portfolio
# 'final_portfolio_c', keeps track of the count of the reviews each permno of each tercile has received in a given month

final_portfolio = pd.DataFrame(columns=('Month', 'T1', 'T2', 'T3'))
final_portfolio_c = pd.DataFrame(columns=('Month', 'T1', 'T2', 'T3'))

all_permnos = list(abnormal_matrix['LPERMNO'])
print(len(all_permnos))
abn_matr_trans = abnormal_matrix.transpose()  # TODO: this should be deleted, just for visual check
for month in islice(abnormal_matrix.transpose().itertuples(), 1, None):
    abnormal = []  # For each month this list will contain the 'abnormal ratings' (i.e. deviations from
                   # past 12 year_month mean) not equal to 'nan'
    temp_permnos = []  # and this one the corresponding permons.
    counts_ratings = []  # This will get the number of counts (i.e. ratings) to do the weighting later on.
    i = -1
    MONTHYEAR_ABNORMAL_MATRIX = 0
    for x in month[1:]:  # here we get for each month the abnormal ratings across all permnos. Each row has in the first
                         # column the month and in the following columns each entry represents the abnormal
                         # rating in that month for a specific permno
        i += 1
        if str(x) != 'nan':
            try:
                abnormal.append(float(x))  # append the abnormal rating
                temp_permnos.append(all_permnos[i])  # append the corresponding permno
                counts_ratings.append(
                    number_reviews[str(month[MONTHYEAR_ABNORMAL_MATRIX])][
                        list(number_reviews['LPERMNO']).index(all_permnos[i])]) # append the corresponding number
                                                                                # of reviews
            except:
                print("There was a 'NaN'")
    print(abnormal)
    # sort and divide the observations in three roughly equal groups to build the terciles
    if len(abnormal) != 0:
        abnormal, temp_permnos, counts_ratings = (list(t) for t in
                                                  zip(*sorted(zip(abnormal, temp_permnos, counts_ratings))))
        div = round(len(abnormal) / 3)

        t1 = temp_permnos[:div]
        t2 = temp_permnos[div:2 * div]
        t3 = temp_permnos[2 * div:]
        portfolios = [month[MONTHYEAR_ABNORMAL_MATRIX], t1, t2, t3]  # TODO: check how many portfolios do not have one
                                                                     #    value per tercile. This might cause problems
                                                                     #    in the econometric analysis
        prov = pd.Series(portfolios, index=final_portfolio.columns)
        final_portfolio = final_portfolio.append(prov, ignore_index=True)

        ct1 = counts_ratings[:div]
        ct2 = counts_ratings[div:2 * div]
        ct3 = counts_ratings[2 * div:]
        portfolios_c = [month[MONTHYEAR_ABNORMAL_MATRIX], ct1, ct2, ct3]
        prov_c = pd.Series(portfolios_c, index=final_portfolio_c.columns)
        final_portfolio_c = final_portfolio_c.append(prov_c, ignore_index=True)

# save final_portfolio and final_portfolio_c for debugging
final_portfolio.to_csv(
    r"../Amazon Project - Data\support_data\CKB replication/final_portfolio_wratio_5votes.csv", index=False)
print("final_portfolio done")

final_portfolio_c.to_csv(
    r"../Amazon Project - Data\support_data\CKB replication/final_portfolio_c_wratio_5votes.csv", index=False)
print("final_portfolio_c done")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 5: In this section we bring together the tercile portfolios and excess returns
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# Mattia's OS path:   /Users/mattia/Dropbox (IESE)/Useful Material/Amazon Project/Data/Auxiliary/msf_riskfree_brands.csv
# Mattia's Windows path:   D:\Dropbox (IESE)\Useful Material\Amazon Project\Data\Auxiliary\msf_riskfree_brands.csv
permnos_returns = pd.read_csv(
    r"../../Amazon Project - Data/support_data/CKB replication/excess_returns_wratio_5votes.csv")
factors_4CFF = pd.read_csv(r"../../Amazon Project - Data/support_data/CKB replication/factors_4CFF.csv")

# Load portfolios for debugging]
final_portfolio = pd.read_csv(
    r"../../Amazon Project - Data/support_data/CKB replication/final_portfolio_wratio_5votes.csv")
final_portfolio_c = pd.read_csv(
    r"../../Amazon Project - Data/support_data/CKB replication/final_portfolio_c_wratio_5votes.csv")


# the idea now could be to go in 'final_portfolio', extract each month the permnos that are in each portfolio and go
# into 'permnos_returns' and join if yyyymm and permnos are the same in both. We could have for output a df where:
# -     the first column we have yyyymm
# -     the following columns T1(company) T1(permno) T2(company) T2(permno) T3(company) T3(permnos)

# We define two functions useful in the next for-loop:
#   -   flatten: used to 'flatten' lists (i.e. make nested lists into a single-level list)
#   -   keep_running: used to keep the program running if there are no permnos in a cell
def flatten(lis):
    for item in lis:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in flatten(item):
                yield x
        else:
            yield item


def keep_running(name):
    if name != '[]':
        return [float(i) for i in name[1:-1].split(', ')]
    else:
        pass


# The following lines divide the terciles into three different DataFrames, each containing the month and the
# permnos pertaining to that tercile in that month. The main output is:
#   -   date_T1_permnos, contains two columns: 'Month' and 'T1'. The date (YYYYMM) and the permnos in the first
#                           tercile that month
#   -   date_T2_permnos, contains two columns: 'Month' and 'T2'. The date (YYYYMM) and the permnos in the second
#                           tercile that month
#   -   date_T3_permnos, contains two columns: 'Month' and 'T3'. The date (YYYYMM) and the permnos in the third
#                           tercile that month

date_T1_permnos = pd.DataFrame()
date_T2_permnos = pd.DataFrame()
date_T3_permnos = pd.DataFrame()

date_T1_permnos['Month'] = final_portfolio['Month']
date_T1_permnos['T1'] = final_portfolio['T1'].str[1:-1]

date_T2_permnos['Month'] = final_portfolio['Month']
date_T2_permnos['T2'] = final_portfolio['T2'].str[1:-1]

date_T3_permnos['Month'] = final_portfolio['Month']
date_T3_permnos['T3'] = final_portfolio['T3'].str[1:-1]

# The following work in the same fashion as the previous lines, but get the count for how many reviews
# each permno in each tercile has had in a specific month.
date_T1_count = pd.DataFrame()
date_T2_count = pd.DataFrame()
date_T3_count = pd.DataFrame()

date_T1_count['Month'] = final_portfolio_c['Month']
date_T1_count['T1'] = final_portfolio_c['T1'].str[1:-1]

date_T2_count['Month'] = final_portfolio_c['Month']
date_T2_count['T2'] = final_portfolio_c['T2'].str[1:-1]

date_T3_count['Month'] = final_portfolio_c['Month']
date_T3_count['T3'] = final_portfolio_c['T3'].str[1:-1]

# in the for-loop that follows we take date_T1_permnos, date_T2_permnos, date_T3_permnos and modify them to make them
# suitable for time-series analysis with each row containing only the date and one corresponding permno.

def _string_unpack_(string):
    _string = string.replace(r'\D+', '')
    _list = [_string.split(', ')]
    return list(flatten(_list))


df_list = [date_T1_permnos, date_T2_permnos, date_T3_permnos]

df_T1 = pd.DataFrame()
df_T2 = pd.DataFrame()
df_T3 = pd.DataFrame()

which_df = 0
for df in df_list:
    which_df += 1
    temp_permnos = []
    temp_dates = []
    for row in df.itertuples():
        row_deb = row[2]
        number_list = _string_unpack_(str(row[2]))
        temp_permnos.extend(number_list)

        month_list = []
        month_list.extend(row[1] for i in range(len(number_list)))
        temp_dates.extend(month_list)
        print(number_list)
    temp_dates_series = pd.Series(temp_dates, name='Month')
    temp_permnos_series = pd.Series(temp_permnos, name='T{}'.format(which_df))

    if which_df == 1:
        df_T1 = pd.concat([temp_dates_series, temp_permnos_series], axis=1)
        df_T1['T1'] = pd.to_numeric(df_T1['T1'], errors='coerce')
    elif which_df == 2:
        df_T2 = pd.concat([temp_dates_series, temp_permnos_series], axis=1)
        df_T2['T2'] = pd.to_numeric(df_T2['T2'], errors='coerce')
    elif which_df == 3:
        df_T3 = pd.concat([temp_dates_series, temp_permnos_series], axis=1)
        df_T3['T3'] = pd.to_numeric(df_T3['T3'], errors='coerce')

# the next three merges bring together each permno in each month, divided by terciles, with the corresponding
# excess returns for the Fama-French_Carhartt four factor model regressions.

columns_drop = ['Month', 'RF_x', 'date_y']

columns_rename = {'date_x': 'Date', 'COMNAM': 'Company Name', 'RF_y': 'RF', 'Mom': 'MOM'}

columns_order = ['Date', 'PERMNO', 'Company Name', 'RET', 'XRET', 'Mkt_RF', 'SMB', 'HML', 'MOM', 'RF',
                 'CUSIP', 'SICCD']

df_T1.Month = df_T1.Month.astype('int64')
final_T1 = df_T1.merge(permnos_returns, how='inner', left_on=['Month', 'T1'], right_on=['date', 'PERMNO']).merge(
    factors_4CFF, how='inner', left_on='Month', right_on='date')
final_T1 = final_T1.drop(columns_drop + ['T1'], axis=1)
final_T1 = final_T1.rename(columns=columns_rename)
final_T1 = final_T1[columns_order]

df_T2.Month = df_T2.Month.astype('int64')
final_T2 = df_T2.merge(permnos_returns, how='inner', left_on=['Month', 'T2'], right_on=['date', 'PERMNO']).merge(
    factors_4CFF, how='inner', left_on='Month', right_on='date')
final_T2 = final_T2.drop(columns_drop + ['T2'], axis=1)
final_T2 = final_T2.rename(columns=columns_rename)
final_T2 = final_T2[columns_order]

df_T3.Month = df_T3.Month.astype('int64')
final_T3 = df_T3.merge(permnos_returns, how='inner', left_on=['Month', 'T3'], right_on=['date', 'PERMNO']).merge(
    factors_4CFF, how='inner', left_on='Month', right_on='date')
final_T3 = final_T3.drop(columns_drop + ['T3'], axis=1)
final_T3 = final_T3.rename(columns=columns_rename)
final_T3 = final_T3[columns_order]

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 6: Build the T1, T2, T3 excess portfolio returns
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# Below we compute the portfolios
final_T1_harmonized_date = final_T1.copy()
final_T2_harmonized_date = final_T2.copy()
final_T3_harmonized_date = final_T3.copy()

# see what elements are not in common (T1 and T3 are the drivers here since we have to compute the long/short
# portfolios later)
col_T1 = final_T1_harmonized_date['Date'].tolist()
col_T3 = final_T3_harmonized_date['Date'].tolist()
difference = list(set(col_T1) - set(col_T3))
difference.sort()

# mask out dates that are not in common to have homogeneous time-frame
mask_T1T3 = final_T1_harmonized_date.Date.isin(final_T3_harmonized_date.Date)
mask_T3T1 = final_T3_harmonized_date.Date.isin(final_T1_harmonized_date.Date)

final_T1_harmonized_date_masked = final_T1_harmonized_date[mask_T1T3]
final_T3_harmonized_date_masked = final_T3_harmonized_date[mask_T3T1]

mask_T2T3 = final_T2_harmonized_date.Date.isin(final_T3_harmonized_date_masked.Date)
final_T2_harmonized_date_masked = final_T2_harmonized_date[mask_T2T3]

# all terciles with same consecutive common dates
final_T1_consec_dates = final_T1_harmonized_date_masked
final_T1_consec_dates.drop(
    final_T1_consec_dates[final_T1_consec_dates.Date < 200801].index, inplace=True)

final_T2_consec_dates = final_T2_harmonized_date_masked
final_T2_consec_dates.drop(
    final_T2_consec_dates[final_T2_consec_dates.Date < 200801].index, inplace=True)

final_T3_consec_dates = final_T3_harmonized_date_masked
final_T3_consec_dates.drop(
    final_T3_consec_dates[final_T3_consec_dates.Date < 200801].index, inplace=True)

# Unweighted portfolios
xret_T1_mean = final_T1_consec_dates.groupby('Date', as_index=False)['XRET'].mean()
xret_T2_mean = final_T2_consec_dates.groupby('Date', as_index=False)['XRET'].mean()
xret_T3_mean = final_T3_consec_dates.groupby('Date', as_index=False)['XRET'].mean()

T1_portfolio = pd.merge(xret_T1_mean, factors_4CFF, left_on='Date', right_on='date', how='inner')
T2_portfolio = pd.merge(xret_T2_mean, factors_4CFF, left_on='Date', right_on='date', how='inner')
T3_portfolio = pd.merge(xret_T3_mean, factors_4CFF, left_on='Date', right_on='date', how='inner')

T1_portfolio.drop(columns='date', axis=1, inplace=True)
T2_portfolio.drop(columns='date', axis=1, inplace=True)
T3_portfolio.drop(columns='date', axis=1, inplace=True)

T1_portfolio.to_stata(r'../Amazon Project - Data/Results/T1_portfolio.dta', write_index=False)
T2_portfolio.to_stata(r'../Amazon Project - Data/Results/T2_portfolio.dta', write_index=False)
T3_portfolio.to_stata(r'../Amazon Project - Data/Results/T3_portfolio.dta', write_index=False)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 7: Build the T1, T2, T3 weighted excess portfolio returns
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# Now we repeat the same procedure for date_T1_count, date_T2_count, date_T3_count
df_list2 = [date_T1_count, date_T2_count, date_T3_count]

df_T1_c = pd.DataFrame()
df_T2_c = pd.DataFrame()
df_T3_c = pd.DataFrame()

which_df = 0
for df in df_list2:
    which_df += 1
    temp_permnos = []
    temp_dates = []
    for row in df.itertuples():
        row_deb = row[2]
        number_list = _string_unpack_(str(row[2]))
        temp_permnos.extend(number_list)

        month_list = []
        month_list.extend(row[1] for i in range(len(number_list)))
        temp_dates.extend(month_list)
        print(number_list)
    temp_dates_series = pd.Series(temp_dates, name='Month')
    temp_permnos_series = pd.Series(temp_permnos, name='T{}'.format(which_df))

    if which_df == 1:
        df_T1_c = pd.concat([temp_dates_series, temp_permnos_series], axis=1)
        df_T1_c['T1'] = pd.to_numeric(df_T1_c['T1'], errors='coerce')
    elif which_df == 2:
        df_T2_c = pd.concat([temp_dates_series, temp_permnos_series], axis=1)
        df_T2_c['T2'] = pd.to_numeric(df_T2_c['T2'], errors='coerce')
    elif which_df == 3:
        df_T3_c = pd.concat([temp_dates_series, temp_permnos_series], axis=1)
        df_T3_c['T3'] = pd.to_numeric(df_T3_c['T3'], errors='coerce')

df_T1_w = df_T1.copy()
df_T2_w = df_T2.copy()
df_T3_w = df_T3.copy()

df_T1_w['number_reviews'] = df_T1_c['T1'].copy()
df_T2_w['number_reviews'] = df_T2_c['T2'].copy()
df_T3_w['number_reviews'] = df_T3_c['T3'].copy()

df_T1_w_sumrev = df_T1_w.groupby('Month', as_index=False)['number_reviews'].sum()
df_T2_w_sumrev = df_T2_w.groupby('Month', as_index=False)['number_reviews'].sum()
df_T3_w_sumrev = df_T3_w.groupby('Month', as_index=False)['number_reviews'].sum()

df_T1_sum_reviews = pd.merge(df_T1_w, df_T1_w_sumrev, on='Month', how='inner')
df_T2_sum_reviews = pd.merge(df_T2_w, df_T2_w_sumrev, on='Month', how='inner')
df_T3_sum_reviews = pd.merge(df_T3_w, df_T3_w_sumrev, on='Month', how='inner')

df_T1_sum_reviews = df_T1_sum_reviews.rename(columns={'number_reviews_x':'monthly_reviews', 'number_reviews_y':'total_monthly_reviews'})
df_T2_sum_reviews = df_T2_sum_reviews.rename(columns={'number_reviews_x':'monthly_reviews', 'number_reviews_y':'total_monthly_reviews'})
df_T3_sum_reviews = df_T3_sum_reviews.rename(columns={'number_reviews_x':'monthly_reviews', 'number_reviews_y':'total_monthly_reviews'})

df_T1_sum_reviews['weight_reviews'] = df_T1_sum_reviews['monthly_reviews'] / df_T1_sum_reviews['total_monthly_reviews']
df_T2_sum_reviews['weight_reviews'] = df_T2_sum_reviews['monthly_reviews'] / df_T2_sum_reviews['total_monthly_reviews']
df_T3_sum_reviews['weight_reviews'] = df_T3_sum_reviews['monthly_reviews'] / df_T3_sum_reviews['total_monthly_reviews']

# the next three merges bring together each permno in each month, divided by terciles, with the corresponding
# excess returns for the Fama-French_Carhartt four factor model regressions.
columns_drop = ['Month', 'RF_x', 'date_y']

columns_rename = {'date_x': 'Date', 'COMNAM': 'Company Name', 'RF_y': 'RF', 'Mom': 'MOM'}

columns_order = ['Date', 'PERMNO', 'Company Name', 'weight_reviews', 'monthly_reviews', 'total_monthly_reviews',
                'RET', 'XRET', 'Mkt_RF', 'SMB', 'HML', 'MOM', 'RF', 'CUSIP', 'SICCD']

final_T1_weight = df_T1_sum_reviews.merge(permnos_returns, how='inner', left_on=['Month', 'T1'], right_on=['date', 'PERMNO']).merge(
    factors_4CFF, how='inner', left_on='Month', right_on='date')
final_T1_weight = final_T1_weight.drop(columns_drop + ['T1'], axis=1)
final_T1_weight = final_T1_weight.rename(columns=columns_rename)
final_T1_weight = final_T1_weight[columns_order]

final_T2_weight = df_T2_sum_reviews.merge(permnos_returns, how='inner', left_on=['Month', 'T2'], right_on=['date', 'PERMNO']).merge(
    factors_4CFF, how='inner', left_on='Month', right_on='date')
final_T2_weight = final_T2_weight.drop(columns_drop + ['T2'], axis=1)
final_T2_weight = final_T2_weight.rename(columns=columns_rename)
final_T2_weight = final_T2_weight[columns_order]

final_T3_weight = df_T3_sum_reviews.merge(permnos_returns, how='inner', left_on=['Month', 'T3'], right_on=['date', 'PERMNO']).merge(
    factors_4CFF, how='inner', left_on='Month', right_on='date')
final_T3_weight = final_T3_weight.drop(columns_drop + ['T3'], axis=1)
final_T3_weight = final_T3_weight.rename(columns=columns_rename)
final_T3_weight = final_T3_weight[columns_order]

T1_XRET_weighted = final_T1_weight['XRET'] * final_T1_weight['weight_reviews']
final_T1_weight.insert(loc=8, column='XRET_weighted', value=T1_XRET_weighted)

T2_XRET_weighted = final_T2_weight['XRET'] * final_T2_weight['weight_reviews']
final_T2_weight.insert(loc=8, column='XRET_weighted', value=T2_XRET_weighted)

T3_XRET_weighted = final_T3_weight['XRET'] * final_T3_weight['weight_reviews']
final_T3_weight.insert(loc=8, column='XRET_weighted', value=T3_XRET_weighted)

# Below we compute the portfolios
final_T1_harmonized_date_w = final_T1_weight.copy()
final_T2_harmonized_date_w = final_T2_weight.copy()
final_T3_harmonized_date_w = final_T3_weight.copy()

# see what elements are not in common (T1 and T3 are the drivers here since we have to compute the long/short
# portfolios later)
col_T1 = final_T1_harmonized_date_w['Date'].tolist()
col_T3 = final_T3_harmonized_date_w['Date'].tolist()
difference = list(set(col_T1) - set(col_T3))
difference.sort()

# mask out dates that are not in common to have homogeneous time-frame
mask_T1T3_w = final_T1_harmonized_date_w.Date.isin(final_T3_harmonized_date_w.Date)
mask_T3T1_w = final_T3_harmonized_date_w.Date.isin(final_T1_harmonized_date_w.Date)

final_T1_harmonized_date_w_masked = final_T1_harmonized_date_w[mask_T1T3_w]
final_T3_harmonized_date_w_masked = final_T3_harmonized_date_w[mask_T3T1_w]

mask_T2T3_w = final_T2_harmonized_date_w.Date.isin(final_T3_harmonized_date_w_masked.Date)
final_T2_harmonized_date_w_masked = final_T2_harmonized_date_w[mask_T2T3_w]

# all terciles with same consecutive common dates
final_T1_consec_dates_w = final_T1_harmonized_date_w_masked
final_T1_consec_dates_w.drop(
    final_T1_consec_dates_w[final_T1_consec_dates_w.Date < 200801].index, inplace=True)

final_T2_consec_dates_w = final_T2_harmonized_date_w_masked
final_T2_consec_dates_w.drop(
    final_T2_consec_dates_w[final_T2_consec_dates_w.Date < 200801].index, inplace=True)

final_T3_consec_dates_w = final_T3_harmonized_date_w_masked
final_T3_consec_dates_w.drop(
    final_T3_consec_dates_w[final_T3_consec_dates_w.Date < 200801].index, inplace=True)

# Weighted portfolios
xret_T1_mean_w = final_T1_consec_dates_w.groupby('Date', as_index=False)['XRET_weighted'].sum()
xret_T2_mean_w = final_T2_consec_dates_w.groupby('Date', as_index=False)['XRET_weighted'].sum()
xret_T3_mean_w = final_T3_consec_dates_w.groupby('Date', as_index=False)['XRET_weighted'].sum()

T1_portfolio_w = pd.merge(xret_T1_mean_w, factors_4CFF, left_on='Date', right_on='date', how='inner')
T2_portfolio_w = pd.merge(xret_T2_mean_w, factors_4CFF, left_on='Date', right_on='date', how='inner')
T3_portfolio_w = pd.merge(xret_T3_mean_w, factors_4CFF, left_on='Date', right_on='date', how='inner')

T1_portfolio_w.drop(columns='date', axis=1, inplace=True)
T2_portfolio_w.drop(columns='date', axis=1, inplace=True)
T3_portfolio_w.drop(columns='date', axis=1, inplace=True)

T1_portfolio_w.to_stata(r'../Amazon Project - Data/Results/T1_portfolio_weighted.dta', write_index=False)
T2_portfolio_w.to_stata(r'../Amazon Project - Data/Results/T2_portfolio_weighted.dta', write_index=False)
T3_portfolio_w.to_stata(r'../Amazon Project - Data/Results/T3_portfolio_weighted.dta', write_index=False)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 8: Build the "Long/Short" portfolio
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# 'long_short_portfolio' contains dates and XRET for the long short portfolio
l_s_portfolio = pd.DataFrame()
l_s_portfolio['Date'] = xret_T3_mean['Date'].copy()
l_s_xret = (xret_T3_mean['XRET'] - xret_T1_mean['XRET']) / 2
l_s_portfolio['XRET'] = l_s_xret
long_short_portfolio = pd.merge(l_s_portfolio, factors_4CFF, left_on='Date', right_on='date', how='inner')
long_short_portfolio.to_stata(r'../Amazon Project - Data/Results/long_short_portfolio.dta', write_index=False)

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
• Section 9: Build the weighted "Long/Short" portfolio
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# 'long_short_portfolio' contains dates and XRET for the long short portfolio
l_s_portfolio_w = pd.DataFrame()
l_s_portfolio_w['Date'] = xret_T3_mean_w['Date']
l_s_xret_w = (xret_T3_mean_w['XRET_weighted'] - xret_T1_mean_w['XRET_weighted']) / 2
l_s_portfolio_w['XRET_weighted'] = l_s_xret_w
long_short_portfolio_w = pd.merge(l_s_portfolio_w, factors_4CFF, left_on='Date', right_on='date', how='inner')
long_short_portfolio_w.to_stata(r'../Amazon Project - Data/Results/long_short_portfolio_weighted.dta', write_index=False)

print("The code has finished")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# ======================================================================================================================
# ======================================================================================================================
#           ---------------------------   SCRAP CODE REPOSITORY FROM HERE ON   ---------------------------
# ======================================================================================================================
# ======================================================================================================================


# ======================================================================================================================
#   In this section we bring together the tercile portfolios and escess returns
# ======================================================================================================================


# date_terciles = dateT1.merge(dateT2, on = 'Month', how = 'outer').merge(dateT3, on = 'Month', how = 'outer')

# fin_brand_terc1 = permnos_returns.merge(date_terciles, )

# elements = el.split(',')

# row[1] =

# for el in row[1:3]:
#     elements = list(el)
#     if elements in row[1]:
#         for i in elements:
#             prov_dateT1 = pd.Series(i, index = date_terc_brands1['T1'])
#             date_terc_brands1 = date_terc_brands1.append(row[0], prov_dateT1)
#     elif elements in row[2]:
#         for i in elements:
#             prov_dateT2 = pd.Series(elements, index = date_terc_brands2['T2'])
#             date_terc_brands2 = date_terc_brands2.append(row[0], prov_dateT2)
#     else:
#         prov_dateT3 = pd.Series(elements, index = date_terc_brands3['T3'])
#         dateT3 = dateT3.append(row[0], prov_dateT3)

# date_terc_brands1 = date_terc_brands1.append(dateT1, index = date_terc_brands1.columns)
# date_terc_brands2 = date_terc_brands2.append(dateT2, index = date_terc_brands2.columns)
# date_terc_brands3 = date_terc_brands3.append(dateT3, index = date_terc_brands3.columns)

# with open(r'D:\Dropbox (IESE)\Useful Material\Amazon Project\Data\Meta_reviews_merged\Grocery_m.csv', 'r', encoding = 'utf-8', newline = '') as myfile:
#     next(myfile) # Skips first row
#     for row in csv.reader(myfile, delimiter = ','):
# # ====================================================================================================================
# #
# #   Extract categories (to reduce the number of elements to check with fuzzywuzzy)
# #   Compare 'category' in meta_reviews with 'main_cat' in grocery company codes and pick the most similar one, ie higher score
# #   (in this case it doesn't matter cause we only have 'Grocery' data, but should
# #   speed up the process with the whole sample.)
# #
# #   !!! MAYBE WE CAN SKIP THIS STEP SINCE THE DATASETS ARE IN DIFFERENT FILES EPARATED BY CATEGORIES !!!
# #
# # ====================================================================================================================
#         temp_categories = row[0][1:-1].replace(',' , '').split("'")
#         cat_score = pd.DataFrame(columns = ('Cat', 'Score'))

#         for t_cat in temp_categories:
#             for cat in categories:
#                 to_append = [cat, fuzz.partial_ratio(t_cat.lower(),cat.lower())]
#                 prov = pd.Series(to_append, index = cat_score.columns)
#                 cat_score = cat_score.append(prov, ignore_index = True)

#         category = cat_score['Cat'][list(cat_score['Score']).index(max(list(cat_score['Score'])))]
#         print(category + '  ' + str(max(list(cat_score['Score']))))


# number_reviews_merged_5.to_csv(r'../Amazon Project - Data/support_data/To send to Miguel/25_03_21/monthly_number_reviews_280permnos.csv')
# average_overall_merged_5.to_csv(r'../Amazon Project - Data/support_data/To send to Miguel/25_03_21/average_monthly_rating_280permnos.csv')
#
# mask5_2 = csv_target_file.LPERMNO.isin(number_reviews_merged_5.LPERMNO)
# csv_target_file_atleast5 = csv_target_file[mask5_2]
# csv_target_file_atleast5.to_csv(r'../Amazon Project - Data/support_data/To send to Miguel/25_03_21/comp_brand_link_ratings_280permnos.csv')
# no_dup = csv_target_file_atleast5.drop_duplicates(subset=['LPERMNO'])
# no_dup.to_csv(r'../Amazon Project - Data/support_data/To send to Miguel/25_03_21/comp_brand_280permnos.csv')
