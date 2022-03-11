import pandas as pd
import os
from Code._helpers.setup import trademark_case_files_path, ROOT_DIR


path = trademark_case_files_path
case_files_main_path = os.path.join(trademark_case_files_path, 'case_files_main.csv')
owner_main_path = trademark_case_files_path + '/owner_main.csv'
classification_path = os.path.join(trademark_case_files_path, 'classification.dta')

# load files  and merge
case_files_main = pd.read_csv(case_files_main_path, low_memory=False)
owner_main = pd.read_csv(owner_main_path, low_memory=False)

td_with_owner = pd.merge(case_files_main, owner_main, on='serial_no', how='inner')
td_with_owner.to_csv(os.path.join(trademark_case_files_path, 'casefiles_owner_main.csv'), index=False)