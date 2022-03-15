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
from scripts.helpers.setup import trademark_case_files_path, processed_data_dir, test_dir
import pandas as pd
import time

""" 
Setting directories
"""
# local directory
# dir_load_amz_brands = os.path.join(trademark_case_files_path, 'casefiles_owner_nodup_nonans.csv')
# dir_save = os.path.join(processed_data_dir, 'matching_firms_and_brands/match_trademarks_amazon')
# df_tdmks = pd.read_csv(dir_load_amz_brands, low_memory=False)

# test directory
dir_load_amz_brands = os.path.join(test_dir, 'tests_load/metadata_test_load')
dir_save = os.path.join(processed_data_dir, 'test_save/trademarks_test/test_cossim_matching')
dir_trademarks = os.path.join(trademark_case_files_path, 'casefiles_owner_nodup_nonans.csv')

"""
Start code
"""


# ngrams create ngrams for each string fed to it
def ngrams(string, n=3):
    string = re.sub(r'[,-./]|\sBD', r'', string)
    get_ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in get_ngrams]


# unpack the resulting sparse matrix from 'awesome_cossim_topn' function. 
# As it is a bit slow, an option to look at only the first n values is added.
def get_matches_df(sparse_matrix, name_vector, top=100):
    non_zeros = sparse_matrix.nonzero()

    sparserows = non_zeros[0]
    sparsecols = non_zeros[1]

    if top:
        nr_matches = top
    else:
        nr_matches = sparsecols.size

    left_side = np.empty([nr_matches], dtype=object)
    right_side = np.empty([nr_matches], dtype=object)
    similarity = np.zeros(nr_matches)

    for index in range(0, nr_matches):
        left_side[index] = name_vector[sparserows[index]]
        right_side[index] = name_vector[sparsecols[index]]
        similarity[index] = sparse_matrix.data[index]

    return pd.DataFrame({'left_side': left_side,
                         'right_side': right_side,
                         'similarity': similarity})


# load trademarks in pandas
df_tdmks = pd.read_csv(dir_trademarks, low_memory=False)
tdmks_names = df_tdmks['mark_id_char']
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
tf_idf_matrix_tdmks = vectorizer.fit_transform(tdmks_names)

random_samples = pd.DataFrame()
top_results = pd.DataFrame()
iteration_number = 0
for file in os.listdir(dir_load_amz_brands):
    iteration_number += 1

    filename = "/" + file
    amazon_df = MainFunc.get_df(dir_load_amz_brands + filename)  # function to open json file as df
    print('We are in file: {}'.format(str(file)))
    start_time = time.time()

    brand_names = amazon_df['brand']
    tf_idf_matrix_amz_brands = vectorizer.fit_transform(brand_names)
    matches = awesome_cossim_topn(tf_idf_matrix_tdmks, tf_idf_matrix_amz_brands, 10, 0.8)
    finish_time_match = time.time() - start_time
    time_matching_seconds = finish_time_match / 60
    print('-----    MATCHING file n.{}, {}, took {} seconds    -----'.format(iteration_number, file, 
                                                                             time_matching_seconds))

    matches_df = get_matches_df(matches, tdmks_names, top=100000)
    finish_time_unpack = time.time() - finish_time_match
    time_unpacking_seconds = finish_time_unpack / 60 
    print('-----    UNPACKING file n.{}, {}, took {} seconds    -----'.format(iteration_number, file, 
                                                                             time_unpacking_seconds))
    # matches_df = matches_df[matches_df['similarity'] < 0.99999]  # Remove all exact matches
    sample_match = matches_df.sample(20)
    random_samples = random_samples.append(sample_match)

    top_test = matches_df.sort_values(['similarity'], ascending=False).head(10)
    top_results = top_results.append(top_test)
    