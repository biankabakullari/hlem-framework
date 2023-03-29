import logging
import hlem_with_paths as hlem_paths
import process_bpic_2017 as process_bpic
import hl_paths.postprocess as postprocess


# ============= The following parameters are fixed for all experiments =============
from hl_paths import case_participation

traffic_type = 'High'  # means we are interested in particularly "high" values
selected_f_list = hlem_paths.DEFAULT_HLF  # set to ['enter', 'exit', 'handover', 'workload', 'batch', 'delay']
res_info = True  # assuming the log has resource info, otherwise 'handover' and 'workload' won't be computed
act_selection = 'all'  # we want to consider all activities and thus all segments in the process
# this has no effect on the result
seg_method = 'df'  # means the steps we consider (event pairs traversing a segment) are the directly follows event pairs
type_based = False  # if True, all e.g. "enter" hle have the same threshold and runtime is smaller

# ============= The following parameters can be configured =============
p = 0.9  # the extremity threshold, the values above the pth percentile will generate a hle
co_thresh = 0.5  # the case overlap threshold for connecting hle pairs
co_path_thresh = 0.5  # the case overlap threshold for a path of hle, should not be higher than co_thresh
only_maximal_paths = True  # consider only maximal paths
path_freq = 10  # if 0 consider all hla paths, if a number > 0, consider only hla paths with frequency count above
frame = 'days'  # how to partition the time space onto windows
seg_percentile = 0.9  # detect only hle over segments that are at least as frequent as the p*100th percentile


def download_tables(path_to_bpic2017):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    log, res_selection, success_cases, non_success_cases, cases_under_10, cases_10_to_30, cases_over_30, under_10k, \
    over_10k = process_bpic.preprocess_bpic_2017(path_to_bpic2017)

    hle_all_dict, hla_paths_dict = hlem_paths.paths_and_cases_with_overlap(input_log=log, frame=frame,
                                                                             traffic_type=traffic_type,
                                                                             selected_f_list=selected_f_list, p=p,
                                                                             co_thresh=0.5, co_path_thresh=0.5,
                                                                             res_info=True,
                                                                             only_maximal_paths=only_maximal_paths,
                                                                             path_frequency=path_freq,
                                                                             act_selection=act_selection,
                                                                             res_selection=res_selection,
                                                                             seg_method=seg_method,
                                                                             type_based=type_based,
                                                                             seg_percentile=seg_percentile)

    cf_dict = case_participation.get_cf_dict(log)

    # Here two tables are downloaded:
    # a table of all high-level events (and relevant info)
    # a table of all paths which satisfy the path_frequency threshold (and relevant info)
    # the name of the table with paths is returned (which will be used below for correlation computation)
    df_paths = postprocess.gather_statistics(hle_all_dict, hla_paths_dict, cf_dict, p, co_thresh)

    logging.info('Exited successfully.')

    outcome_success = [success_cases, non_success_cases]
    outcome_throughput = [cases_under_10, cases_10_to_30, cases_over_30]
    amount_partition = [under_10k, over_10k]

    return df_paths, outcome_success, outcome_throughput, amount_partition


if __name__ == '__main__':
    print("Make sure you have set the path your local directory of the BPIC 2017 log")
    path_to_bpic2017 = r'C:\Users\bakullari\Documents\hlem_framework\event_logs\BPI-Challenge-2017.xes'
    df_paths, outcome_success, outcome_throughput, amount_partition = download_tables(path_to_bpic2017)
    process_bpic.success_tables(df_paths, outcome_success)
    process_bpic.throughput_tables(df_paths, outcome_throughput)
    process_bpic.amount_tables(df_paths, amount_partition)
