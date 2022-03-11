"""
The following code gets the brands from the Amazon database; namely from the folder 'Metadata_small-brand_nodup'
that contains brands without duplicates
"""

import pandas as pd
import os
from code.helpers import main_functions
from code.helpers.main_functions import getDF

dir_metadata = "D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Metadata\Metadata_small-brand_nodup"

iteration_number = 0
for file in os.listdir(dir_metadata):
    iteration_number += 1
    filename = "/" + file
    print(iteration_number, ' | ', filename)

    df = getDF(dir_metadata + filename)
    df_brand = pd.DataFrame(df['brand'])
    df_brand.to_json('D:\OneDrive - IESE Business School\Documentos\Amazon Project\Amazon Project - Data\Metadata\Metadata_brands_col' + filename, orient='records', lines=True)
