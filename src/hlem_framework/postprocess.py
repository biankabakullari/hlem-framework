import logging
import math
from multiprocessing.pool import ThreadPool
import pandas as pd
from tqdm import tqdm
import case_participation


def _paths_to_rows(hla_paths_dict, cf_dict):
    df_rows = []
    for path, (path_frequency, participating_cases) in hla_paths_dict.items():
        path_length = len(path)
        hla_path_projection = case_participation.project_path_onto_activity_sequence(path)
        # this is the case pool intersected with case-level properties
        relevant_cases = case_participation.get_case_pool(cf_dict, hla_path_projection)
        non_participating_cases = relevant_cases.difference(participating_cases)  # important (per hla)

        row = [path, path_length, path_frequency, relevant_cases, len(relevant_cases), participating_cases,
               len(participating_cases), non_participating_cases, len(non_participating_cases)]
        df_rows.append(row)

    return df_rows


def gather_statistics(hla_paths_dict, cf_dict, p, co_thresh):
    ordered_keys = list(hla_paths_dict.keys())
    batch_size = 1000
    num_batches = int(math.ceil(len(ordered_keys) / batch_size))

    subdicts = [
        {k: hla_paths_dict[k] for k in ordered_keys[batch_size*i : batch_size*(i+1)]}
        for i in range(num_batches)
    ]

    pool = ThreadPool()
    df_rows = []
    with tqdm(desc='Computing rows in batches', total=num_batches) as pbar:
        for rows in pool.imap_unordered(lambda params: _paths_to_rows(*params), [
            (subdict, cf_dict) for subdict in subdicts
        ]):
            df_rows.extend(rows)
            pbar.update()

    logging.info('Creating dataframe.')
    df = pd.DataFrame(df_rows, columns=['path', 'length', 'frequency', 'relevant cases', '# relevant',
                                        'participating', '# participating', 'non-participating', '# non-participating'])
    file_name = 'p-' + str(p * 100) + 'co-' + str(co_thresh * 100) + '.csv'
    df.to_csv(file_name, index=True, header=True)
