import os
import pickle
import pm4py
import logging
import hl_paths.case_participation as case_participation
import hlem_with_paths as hlem_overlap
import hl_paths.postprocess as postprocess


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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    my_path = r'C:\Users\bakullari\Documents\hlem_framework\event_logs\reqforpayment-filtered.xes'
    cache_path = my_path.replace('.xes', '.pickle')
    if os.path.isfile(cache_path):
        with open(cache_path, 'rb') as f:
            log = pickle.load(f)
    else:
        log = pm4py.read_xes(my_path)
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)

    hle_all_dict, hla_paths_dict = hlem_overlap.paths_and_cases_with_overlap(input_log=log, frame=frame,
                                                                             traffic_type=traffic_type,
                                                                             selected_f_list=selected_f_list,
                                                                             p=p, co_thresh=0.5, co_path_thresh=0.5,
                                                                             res_info=True,
                                                                             only_maximal_paths=only_maximal_paths,
                                                                             path_frequency=path_freq,
                                                                             act_selection=act_selection,
                                                                             res_selection=res_selection,
                                                                             seg_method=seg_method,
                                                                             type_based=type_based,
                                                                             seg_percentile=0)

    cf_dict = case_participation.get_cf_dict(log)
    postprocess.gather_statistics(hle_all_dict, hla_paths_dict, cf_dict, p, co_thresh)
    logging.info('Exited successfully.')


if __name__ == '__main__':
    gather_data()
