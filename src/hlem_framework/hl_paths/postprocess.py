import logging
import math
from multiprocessing.pool import ThreadPool
import pandas as pd
from tqdm import tqdm
import hl_paths.case_participation as case_participation


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


def _hle_to_rows(hle_all_dict):
    df_rows = []
    for hle_id in hle_all_dict.keys():
        hle = hle_all_dict[hle_id]
        f_type = hle['f-type']
        entity = hle['entity']
        activity = (f_type, entity)
        value = hle['value']
        theta = hle['theta']

        row = [hle_id, f_type, entity, activity, value, theta]
        df_rows.append(row)

    return df_rows


def gather_statistics(hle_all_dict, hla_paths_dict, cf_dict, p, co_thresh):

    logging.info('Creating table for high-level events.')
    df_hle_rows = _hle_to_rows(hle_all_dict)
    df_hle = pd.DataFrame(df_hle_rows, columns=['hle-id', 'type', 'segment', 'activity', 'value', 'window'])
    file_name_hle = 'hle'+'p-' + str(p * 100) + 'co-' + str(co_thresh * 100) + '.csv'
    df_hle.to_csv(file_name_hle, index=True, header=True)


# Parallel computation here
    ordered_keys = list(hla_paths_dict.keys())
    batch_size = 100
    num_batches = int(math.ceil(len(ordered_keys) / batch_size))

    subdicts = [
        {k: hla_paths_dict[k] for k in ordered_keys[batch_size*i: batch_size*(i+1)]}
        for i in range(num_batches)
    ]
    logging.info('Creating table for paths.')
    pool = ThreadPool()
    df_paths_rows = []
    with tqdm(desc='Computing rows in batches', total=num_batches) as pbar:
        for rows in pool.imap_unordered(lambda params: _paths_to_rows(*params), [
            (subdict, cf_dict) for subdict in subdicts
        ]):
            df_paths_rows.extend(rows)
            pbar.update()

    df_paths = pd.DataFrame(df_paths_rows, columns=['path', 'length', 'frequency', 'relevant cases', '# relevant',
                                                    'participating', '# participating', 'non-participating',
                                                    '# non-participating'])
    file_name_paths = 'paths'+'p-' + str(p * 100) + 'co-' + str(co_thresh * 100) + '.csv'
    df_paths.to_csv(file_name_paths, index=True, header=True)

    return df_paths


