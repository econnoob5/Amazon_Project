import pandas as pd
from fuzzywuzzy import fuzz
import os

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
DESCRIPTION: the following script takes in as input the excel files with public company names (downloaded from NASDAQ's 
             website in this instance but should be fairly general to be applicable to other datasets, with minor 
             changes) and cleans the list of names by deleting duplicates - note: this method is general and can be 
             applied, as in this case, to deal with imperfect matching between the strings (company names).
FINAL OUTPUTS: - csv files, in number that depends on the number of input files, with the company names dropped
               - csv files with the no duplicate company names (i.e. single-entry)
               - csv file that groups together all the single-entry companies 
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# extract project directory
project_dir = os.path.dirname(os.path.dirname(__file__))
# extract directory of "Firms lists to clean" folder
directory_to_clean = os.path.join(project_dir, "Amazon Project - Data/Firms lists from NASDAQ/Firms lists to clean")

# extract directory of "Amazon Project - Data" folder
dir_cleaned = os.path.dirname(os.path.dirname(directory_to_clean))
# extract directory of "Firms lists cleaned" folder
directory_cleaned = os.path.join(dir_cleaned, "Firms lists from NASDAQ/Firms lists cleaned")

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

    # names_dropped.to_csv(
    #     r"/Users/mattia/OneDrive - IESE Business School/Documentos/Amazon Project/Amazon Project - Data/"
    #     r"Firms lists from NASDAQ/Firms lists cleaned/"
    #     r"Companies dropped/" + "Names_dropped_" + name_dataset[15:] + ".csv")
    # dataset.to_csv(
    #     r"/Users/mattia/OneDrive - IESE Business School/Documentos/Amazon Project/Amazon Project - Data/"
    #     r"Firms lists from NASDAQ/Firms lists cleaned/" + name_dataset + "_" + "nodup" + ".csv",
    #     columns=['Symbol', 'Name', 'Market Cap', 'Country', 'Sector', 'Industry'], index=False)

# in the remaining part of the code all the company names are brought together in a single excel file

Firms_list_complete = pd.DataFrame()
for file in os.listdir(directory_cleaned):
    if file.endswith('nodup.csv'):
        dataset_2 = pd.read_csv(directory_cleaned + '/' + file)
        Firms_list_complete = Firms_list_complete.append(dataset_2)
    else:
        continue

Firms_list_complete.to_csv(directory_cleaned + "/" + "Firms_list_complete.csv")

Firms_list_complete.to_excel(directory_cleaned + "/" + "Firms_list_complete.xlsx")

