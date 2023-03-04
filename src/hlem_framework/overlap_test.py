import os
import pickle
import pandas as pd
import pm4py
import case_participation
import hlem_with_overlap as hlem_overlap

# ============= The following parameters are fixed for all experiments =============
traffic_type = 'High'  # means we are interested in particularly "high" values
selected_f_list = hlem_overlap.DEFAULT_HLF  # set to ['enter', 'exit', 'handover', 'workload', 'batch', 'delay']
res_info = True  # assuming the log has resource info, otherwise 'handover' and 'workload' won't be computed
act_selection = 'all'  # we want to consider all activities and thus all segments in the process
res_selection = 'all'  # no filtering on the resources, as we do not compute resource-based event types,
# this has no effect on the result
seg_method = 'df'  # means the steps we consider (event pairs traversing a segment) are the directly follows event pairs
type_based = False  # if True, runtime is smaller

# ============= The following parameters can/should be configured =============
p = 0.9  # the extremity threshold, the values above the pth percentile will generate a hle
co_thresh = 0.5  # the case overlap threshold for connecting hle pairs
co_path_thresh = 0.5  # the case overlap threshold for a path of hle, should not be higher than co_thresh
only_maximal_paths = True  # by default, consider only maximal paths
path_freq = 0  # if 0 consider all hla paths, if a number > 0, consider only hla paths with frequency count above
frame = 'days'  # how to partition the time space onto windows


def gather_data():
    my_path = r'C:\Users\bakullari\Documents\hlem_framework\event_logs\reqforpayment-filtered.xes'
    cache_path = my_path.replace('.xes', '.pickle')
    if os.path.isfile(cache_path):
        with open(cache_path, 'rb') as f:
            log = pickle.load(f)
    else:
        log = pm4py.read_xes(my_path)
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)

    hla_paths, paths_cases, paths_frequencies = hlem_overlap.paths_and_cases_with_overlap(input_log=log, frame=frame,
                                                                                          traffic_type=traffic_type,
                                                                                          selected_f_list=selected_f_list,
                                                                                          p=p,
                                                                                          co_thresh=0.5,
                                                                                          co_path_thresh=0.5,
                                                                                          res_info=True,
                                                                                          only_maximal_paths=only_maximal_paths,
                                                                                          path_frequency=path_freq,
                                                                                          act_selection=act_selection,
                                                                                          res_selection=res_selection,
                                                                                          seg_method=seg_method,
                                                                                          type_based=type_based)

    print('Gathering statistics.')
    df_rows = []
    evaluation_dict = dict()
    number_paths = len(hla_paths)
    cf_dict = case_participation.get_cf_dict(log)
    for i in range(number_paths):
        print('path ' + str(i) + '/' + str(number_paths))
        path = hla_paths[i]
        path_length = len(path)
        path_frequency = paths_frequencies[i]
        participating_cases = paths_cases[i]  # important (per hla)
        print('projection')
        hla_path_projection = case_participation.project_path_onto_activity_sequence(path)
        # this is the case pool intersected with case-level properties
        print('relevant case pool')
        relevant_cases = case_participation.get_case_pool(cf_dict, hla_path_projection)
        non_participating_cases = relevant_cases.difference(participating_cases)  # important (per hla)
        evaluation_dict[i] = {'path': path, 'participating': participating_cases,
                              'non-participating': non_participating_cases}

        row = [i, path, path_length, path_frequency, relevant_cases, len(relevant_cases), participating_cases,
               len(participating_cases), non_participating_cases, len(non_participating_cases)]
        df_rows.append(row)
    print('Computing dataframe.')
    df = pd.DataFrame(df_rows, columns=['id', 'path', 'length', 'frequency', 'relevant cases', '# relevant',
                                        'participating', '# participating', 'non-participating', '# non-participating'])
    file_name = 'p-' + str(p * 100) + 'co-' + str(co_thresh * 100) + '.csv'
    df.to_csv(file_name, index=False, header=True)
    print('Exited successfully.')


if __name__ == '__main__':
    gather_data()
