"""
Code PURPOSE: to illustrate the non-user-friendly content of the json files for easy understanding.

Code OUTPUT: The following code builds an excel table to display the structure of the 'Reviews' json files.
             - rows: dataset name
             - columns: first column, number of columns in the dataset
                        following columns, name of the json columns
             if a dataset has a certain column the corresponding entry of dataset_name-column_name is a
             'Yes', otherwise a 'No'

Code MECHANICS:
"""

import pandas as pd
import gzip
import json
import os

def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)


def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


# # extract project directory
# PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
# # set directory to 'Metadata'
# metadata_directory = os.path.join(PROJECT_ROOT_DIR, "Amazon Project - Data/Reviews")

# server directory
server_dir = r"/data/users/mlaudi/Amazon/Data/Reviews"

full_list_columns = []
for file in os.listdir(server_dir):
    filename = "/" + file
    df = getDF(server_dir + filename)
    print("Executing {}".format(filename[1:-8]))
    list_columns_df = list(df.columns)
    for element in list_columns_df:
        if element not in full_list_columns:
            full_list_columns.append(element)

print("First loop finished")

df_names_list = []
for file in os.listdir(server_dir):
    filename = file[0:-8]
    df_names_list.append(filename)

print("Second loop finished")

all_col_values = pd.DataFrame(columns=full_list_columns)
number_of_columns =[]
for file in os.listdir(server_dir):
    filename = "/" + file
    df = getDF(server_dir + filename)
    print("Executing {}".format(filename[1:-8]))

    df_columns = df.columns
    col_values=[]
    for col in full_list_columns:
        if col not in df_columns:
            col_values.append('No')
        else:
            col_values.append('Yes')
    temp = pd.Series(col_values, index=full_list_columns)
    all_col_values = all_col_values.append(temp, ignore_index=True)

    num_columns = len(df.columns)
    number_of_columns.append(num_columns)

print("Third loop finished")

all_col_values.insert(0, column='Dataset Name', value=df_names_list)
all_col_values.insert(1, column='Number of Columns', value=number_of_columns)

# Mattia's mac directory
# all_col_values.to_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\reviews_structure.xlsx")

# server directory
all_col_values.to_excel(r"/data/users/mlaudi/Amazon/Data/support_data/reviews_structure.xlsx")

print("Phew, that was a lot of work!")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# ======================================================================================================================
# ======================================================================================================================
#                                          SCRAP CODE REPOSITORY FROM HERE ON
# ======================================================================================================================
# ======================================================================================================================