import pandas as pd
from fuzzywuzzy import fuzz
import os

""""""""""""""""""""""""""""""""""""""""""""" Setting directories """""""""""""""""""""""""""""""""""""""""""""""""""""
# ----------------- test directories -----------------
# set PC test directories
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\\"
TARGET_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_load"
SAVING_FOLDER_ROOT_DIR = r"Amazon Project - Data/Tests/Metadata_test_save"
dir_load = os.path.join(PROJECT_ROOT_DIR, TARGET_FOLDER_ROOT_DIR)
dir_save = os.path.join(PROJECT_ROOT_DIR, SAVING_FOLDER_ROOT_DIR)

# ----------------- full run directories on IESE server -----------------
# dir_load = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Metadata_small"
# dir_save = r"/data/users/mlaudi/Amazon/Amazon Project - Data/Tests/Metadata_small-brand_nodup"


""""""""""""""""""""""""""""""""""""""""""""""""" Matching """""""""""""""""""""""""""""""""""""""""""""""""""""""""


for filename in os.listdir(directory_to_clean):
    name_dataset = os.path.join(directory_to_clean, filename)[139:-4]
    print("Executing dataset: " + name_dataset)

    # import the dataset to work with pandas
    dataset = pd.read_csv(directory_to_clean + '/' + filename)

    # duplicate the company name column with one shift down
    INDEX = 2
    shifted_Name = dataset['Name'].shift(1)
    dataset.insert(loc=INDEX, column='Name_shifted', value=shifted_Name)

    # the following for-loop computes the 'fuzzy ratio' and 'fuzzy partial ratio' between a company name and the
    # previous one in the list. If certain threshold is obtained, meaning that the names are quite similar, then the
    # company name in the following row is deleted and the one in the preceding row is kept.
    excel_row = 1
    names_dropped = pd.DataFrame()
    for index, row in dataset.iterrows():
        excel_row += 1
        comp_name = row[1].split()
        shifted_company_name = str(row[2]).split()
        comp_name_for_ratio = ' '.join(map(str, comp_name[:2]))
        shifted_company_name_for_ratio = ' '.join(map(str, shifted_company_name[:2]))
        name_ratio = fuzz.ratio(comp_name_for_ratio, shifted_company_name_for_ratio)
        name_partial_ratio = fuzz.partial_ratio(comp_name_for_ratio, shifted_company_name_for_ratio)
        if name_ratio >= 80 and name_partial_ratio >= 95:
            temporary_name = pd.Series(' '.join(map(str, comp_name)), name='Company name')
            names_dropped = names_dropped.append(temporary_name, ignore_index=True)
            dataset.drop(index, inplace=True)
        else:
            continue

    names_dropped.to_csv(directory_cleaned + "/Companies dropped/" + "Names_dropped_" + name_dataset[15:] + ".csv")
    dataset.to_csv(directory_cleaned + "/" + name_dataset + "_" + "nodup" + ".csv",
                   columns=['Symbol', 'Name', 'Market Cap', 'Country', 'Sector', 'Industry'], index=False)