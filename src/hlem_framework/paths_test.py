from high_level_paths import hle_co_paths
import pickle
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

with open('prepaths_05_03_2023 00_41_49.pickle', 'rb') as f:
    G, case_set_dic, co_thresh, co_path_thresh = pickle.load(f)

hle_paths = hle_co_paths(G, case_set_dic, co_thresh, co_path_thresh, maximal_only=True)

print(len(hle_paths))
