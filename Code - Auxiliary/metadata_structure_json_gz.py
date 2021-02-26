"""
Code PURPOSE: illustrate the non-user-friendly content of the json files for easy understanding.

Code OUTPUT: The following code builds an excel table to display the structure of the 'Metadata' json files.
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
# # # set directory to 'Metadata'
# metadata_directory = os.path.join(PROJECT_ROOT_DIR, "Amazon Project - Data/Metadata")

# server directory
server_dir = r"/data/users/mlaudi/Amazon/Data/Metadata"

"""
Create Excel file to visualize metadata files column structure
"""

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
# all_col_values.to_excel(r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data\metadata_structure.xlsx")

# server directory
all_col_values.to_excel(r"/data/users/mlaudi/Amazon/Data/support_data/metadata_structure.xlsx")

print("Phew, that was a lot of work!")

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                                    END OF CODE
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# ======================================================================================================================
# ======================================================================================================================
#                                          SCRAP CODE REPOSITORY FROM HERE ON
# ======================================================================================================================
# ======================================================================================================================

# max_columns = 0
# for file in os.listdir(metadata_directory):
#     filename = "/" + file
#     df = getDF(metadata_directory + filename)
#     number_columns = len(df.columns)
#     list_columns_df = list(df.columns)
#     if number_columns > max_columns:
#         max_columns = number_columns
#         print(max_columns)
#     else:
#         pass

# full_list_columns.insert(0, 'df_name')

# for file in os.listdir(metadata_directory):
# filename = "/" + file
# df = getDF(metadata_directory + filename)
# list_columns_df = list(df.columns)
# number_columns = str(len(df.columns))
# file_name = filename[1:-8]
# column_number = str(number_columns)

# prov_file_name = pd.Series(file_name, name='Dataset Name')
# prov_column_number = pd.Series(column_number, name='Number of Columns')
# concat_previous = pd.concat([prov_file_name, prov_column_number], axis=1)
# stacking = pd.concat([concat_previous, concat_previous.unstack().reset_index(drop=True).rename('new')],
#                      axis=1).drop(columns=['Dataset Name', 'Number of Columns']).dropna().reset_index()
# stacking_2 = stacking.drop(columns='index').transpose()
# first_two_columns = first_two_columns.append(stacking_2, axis=1)



# df = getDF(metadata_directory + "/meta_Arts_Crafts_and_Sewing.json.gz")
# json_structure = pd.DataFrame(
#     columns=['File name', 'Number of columns', 'Column 1', 'Column 2', 'Column 3', 'Column 4', 'Column 5', 'Column 6',
#              'Column 7', 'Column 8', 'Column 9', 'Column 10', 'Column 11', 'Column 12', 'Column 13', 'Column 14',
#              'Column 15', 'Column 16', 'Column 16', 'Column 17', 'Column 18'])

# all_col_values['df_name'] = df_names_list
# col_name="df_name"
# first_col = all_col_values.pop(col_name)
# all_col_values.insert(0, col_name, first_col)


# number_columns = len(df.columns)
# file_name = filename[1:-8]
# column_number = str(number_columns)
# column_list = list(df.columns.values)
# prov_file_name = pd.Series(file_name, name='File Name')
# prov_column_number = pd.Series(column_number, name='Number of Columns')
# prov_column_list = pd.Series(column_list, name='Column Names')
# concat_previous = pd.concat([prov_file_name, prov_column_number, prov_column_list], axis=1)
# stacking = pd.concat([concat_previous, concat_previous.unstack().reset_index(drop=True).rename('new')],
#                      axis=1).drop(columns=['File Name', 'Number of Columns', 'Column Names']).dropna().reset_index()
# stacking_2 = stacking.drop(columns='index').transpose()
# c = 16
# while len(stacking_2.columns) < 21:
#     l = len(stacking_2.columns)
#     c += 1
#     stacking_2.insert(c, '{}'.format(c), 'empty')
# else:
#     pass
# json_structure = json_structure.append(stacking_2, ignore_index=True)
# print(file_name + ' - ' + column_number + " " + "columns")
# print(column_list)


# """
# Create Excel workbooks and add details to the workbooks
# """
# # Set paths
#
# # excel_dir_perseus = r"/data/users/mlaudi/Amazon/Data/support_data"
# excel_dir_windows = r"D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\support_data"
#
# # open excel file
# metadata_structure = pd.read_excel(excel_dir_windows + '/metadata_structure.xlsx')
#
# excel_sheet2 = pd.DataFrame()
# metadata_structure_columns = pd.Series(metadata_structure.columns[3:], name='columns')
# excel_sheet2['Column Name'] = metadata_structure_columns
# excel_sheet2['Description'] = ""
#
# wb = load_workbook(excel_dir_windows + '/metadata_structure.xlsx')
# ws = wb['Sheet1']
# ws.title = "Metadata structure"
# wb.save(excel_dir_windows + '/metadata_structure.xlsx')
#
# with pd.ExcelWriter(metadata_structure, engine='xlsxwriter') as writer:
#     excel_sheet2.to_excel(writer, 'Column descriptions')
#     writer.save()