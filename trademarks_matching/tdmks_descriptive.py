import pandas as pd
from scripts.helpers.setup import trademark_case_files_path
import scripts.functions.main_functions as MainFunc

# PATHS
TdmksPath = trademark_case_files_path

# FUNCTIONS

# get similar strings within the file
#   write function here

# ------------

# CODE

TdmksCasefilesOwner = pd.read_csv(TdmksPath + '/casefiles_owner_main.csv', low_memory=False)

# get descriptive info for trademarks
TdmksDfColToList = MainFunc.df_cols_to_list(TdmksCasefilesOwner)
TdmksAnalysisTypesDf = MainFunc.data_types_analysis(TdmksDfColToList)
TdmksDfTypes = TdmksAnalysisTypesDf[0]
TdmksDfErrors = TdmksAnalysisTypesDf[1]

TdmksNoNans = TdmksCasefilesOwner[TdmksCasefilesOwner['mark_id_char'].notnull()]
TdmksUniqueMarks = TdmksNoNans.drop_duplicates(subset=['mark_id_char'])
TdmksUniqueMarksOwner = TdmksNoNans.drop_duplicates(subset=['mark_id_char', 'own_name'])
TdmksUniqueMarks.to_csv(TdmksPath + '/casefiles_owner_nodup_nonans.csv', index=False)

# trademarks_similar
#
# owners_unique