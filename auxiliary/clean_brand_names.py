import pandas as pd
import os
from code.helpers import main_functions
from code.helpers.main_functions import getDF
import re

dir_metadata = "D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Metadata\Metadata_brands_col"

iteration_number = 0
for file in os.listdir(dir_metadata):
    iteration_number += 1

    filename = "/" + file
    print(iteration_number, ' | ', filename)

    df = getDF(dir_metadata + filename)
    l_df_dict = df.to_dict('records')

    # remove leading 'by' and spaces
    l_df_dict1 = [str(row['brand'].replace('by', '')) if str(row['brand']).startswith('by') else str(row['brand']) for row in l_df_dict]
    l_df_dict2 = [el.lstrip() for el in l_df_dict1]
    # remove 'unknown's
    l_unknown = ['Unknown', 'unknown', 'UNKNOWN']
    l_df_dict3 = [x for x in l_df_dict2 if x not in l_unknown]
    # keep only alpha-numeric and white space
    l_df_dict4 = [re.sub('[^0-9a-zA-Z ]+', '', x) for x in l_df_dict3]
    # remove empty strings and set lower case
    l_df_dict5 = [x.lower() for x in l_df_dict4 if x != ""]
    l_df_dict6 = [x for x in l_df_dict5 if x != "none"]

    print('debugging')

