"""
if when installing importing from 'sparse_dot_topn' appears 'ValueError: numpy.ndarray
size changed, may indicate binary incompatibility. Expected 96 from C header, got 88 from PyObject',
uninstall 'sparse_dot_topn' and re-install using 'pip install sparse_dot_topn --no-binary pycocotools'
"""

import numpy as np
from scipy.sparse import csr_matrix
from sparse_dot_topn.awesome_cossim_topn import awesome_cossim_topn
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import os
import scripts.functions.main_functions as MainFunc
from scripts.helpers.setup import trademark_case_files_path, processed_data_dir
import pandas as pd
import time

""" 
Setting directories
"""
# local directory
dir_load_amz_brands = os.path.join(trademark_case_files_path, 'casefiles_owner_nodup_nonans.csv')
dir_save = os.path.join(processed_data_dir, 'matching_firms_and_brands/match_trademarks_amazon')
df_tdmks = pd.read_csv(dir_load_amz_brands, low_memory=False)


def ngrams(string, n=3):
    string = re.sub(r'[,-./]|\sBD', r'', string)
    get_ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in get_ngrams]


iteration_number = 0
for file in os.listdir(dir_load_amz_brands):

    iteration_number += 1

    filename = "/" + file
    âmazon_df = MainFunc.get_df(dir_load_amz_brands + filename)  # function to open json file as df
    start_file = time.time()
