"""
This file defines important functions to simplify work in other scripts.
"""

import pandas as pd
import json
import collections
import numpy as np

"""
Functions description
- parse
-getDF

- df_cols_to_list: DataFrame columns to list

- data_types: receives a list and returns a tuple with types list as first argument and exceptions encountered as second

- data_types_analysis: generalizes data_types for any number of lists. Returns a tuple with first arg a list of
                    dictionaries, one for each input list, with the description of the types, and as second argument a 
                    list of the problematic lists that caused errors
"""

# The following two functions are used to parse the NOT-GZIPPED json files and to open the as a Pandas DataFrame


def parse(path):
    with open(path) as jsonfile:
        for line in jsonfile:
            yield json.loads(line)


def get_df(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


def df_cols_to_list(dataframe):
    all_columns = []
    for (columnName, columnData) in dataframe.iteritems():
        col_title = [columnName]
        col_data = columnData.values
        col_title.extend(col_data)
        all_columns.append(col_title)
    return all_columns


def data_types(input_list):
    types_list, types_exceptions = [], []
    for loop_vars in enumerate(input_list):
        try:
            value = loop_vars[1]
            ty = type(value)
            types_list.append(ty)
        except Exception or ValueError as e:
            tup_except = (loop_vars[0], loop_vars[1])
            dict_e = {tup_except: e}
            types_exceptions.append(dict_e)
    counting_types = collections.Counter(types_list)
    pretty_types = dict(counting_types)
    return_data_types = (pretty_types, types_exceptions)
    return return_data_types


def data_types_analysis(input_list):
    output_analysis = pd.DataFrame(columns=['column_name', 'types', 'unique_types', 'number_types',
                                            'exceptions_datatypes', 'uniques_values', 'n_of_nans'])
    output_exceptions = pd.DataFrame(columns=['column_name', 'error'])
    list_number = 0
    for list_in_input_list in input_list:
        list_number += 1
        # the first element of the list_in_input_list should be the column name as per function 'df_cols_to_list'
        column_name = list_in_input_list[0]
        print('Starting list {}'.format(column_name))
        try:
            print('analysing types')
            tuple_data_types = data_types(list_in_input_list)
            # get list_in_input_list of types from output of function 'data_types'
            get_counter = tuple_data_types[0]
            # get unique types and how many
            un = list(set(get_counter))
            nun = len(un)
            # get exceptions from output of function 'data_types'
            exceptions = tuple_data_types[1]

            # understand number of unique values in each column
            # get list without column name at beginning
            print('analysing unique values')
            new_list = list_in_input_list[1:]
            new_series = pd.Series(new_list)
            print('new_list done')
            unique_in_series = new_series.nunique()
            print('unique in series done')

            # understand number of nan values in each column
            print('analysing nan values')
            series_not_nans = new_series.count()
            series_nans = len(new_series) - series_not_nans
            dict_of_lists = {'column_name': column_name, 'types': get_counter, 'unique_types': un,
                             'number_of_types': nun, 'exceptions_datatypes': exceptions,
                             'uniques_values': unique_in_series, 'number_of_nans': series_nans}
            output_analysis = output_analysis.append(dict_of_lists, ignore_index=True)
        except Exception as e:
            dict_except = {'column_name': column_name, 'error': e}
            output_exceptions = output_exceptions.append(dict_except, ignore_index=True)
        print('Finished list {}'.format(column_name))

    # comfortable if we have column names as index in the output df
    output_analysis = output_analysis.set_index('column_name')
    try:
        output_exceptions = output_exceptions.set_index('column_name')
    except Exception as e:
        print('Could not set index because: ' + str(e))
        print('There were no Exceptions; Exception dataframe empty')

    return_data_types_analysis = (output_analysis, output_exceptions)
    return return_data_types_analysis


def flatten(item):
    flattened = []
    for element in item:
        for val in element:
            flattened.append(val)

    return flattened


def similar_in_column(list):
    print('writing function...')