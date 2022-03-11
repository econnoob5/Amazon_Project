import os

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))
trademark_case_files_path = os.path.join(ROOT_DIR, 'data/cleaned/uspto/trademarks_case_files_dataset')
processed_data_dir = os.path.join(ROOT_DIR, 'data/processed')
