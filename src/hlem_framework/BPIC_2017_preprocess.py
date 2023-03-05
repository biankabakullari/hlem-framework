import os
import pickle
import pm4py
import logging
import case_participation
import hlem_with_overlap as hlem_overlap
import postprocess


# return list of all resources except User1 which is automatic
def get_res_selection(log):
    res_list = []
    for trace in log:
        for event in trace:
            res = event['org:resource']
            res_list.append(res)
    user_1 = ['User_1']
    res_set = set(res_list).difference(set(user_1))
    return res_set


# keep only cases that are completed
# applying this function removes "only" 98 of the 31509 traces
def filter_incomplete_cases(log):
    act_list = ['A_Cancelled', 'A_Pending', 'A_Denied']
    filtered_log = pm4py.filter_event_attribute_values(log, "concept:name", act_list, level="case", retain=True)

    return filtered_log


# one may change the <= and < signs onto < and <=
def partition_on_throughput(log):
    class_under_10 = []
    class_10_to_30 = []
    class_over_30 = []
    for trace in log:
        case_id = trace.attributes['concept:name']
        ts_first = trace[0]['time:timestamp']
        ts_last = trace[len(trace)-1]['time:timestamp']
        throughput = (ts_last - ts_first).days
        if throughput <= 10:
            class_under_10.append(case_id)
        elif throughput < 30:
            class_10_to_30.append(case_id)
        else:
            class_over_30.append(case_id)
    return class_under_10, class_10_to_30, class_over_30


# a case has been successful if it has activity A_Pending
def partition_on_outcome(log):
    successful = []
    unsuccessful = []
    for i, trace in enumerate(log):
        case_id = trace.attributes['concept:name']
        control_flow = [event['concept:name'] for event in trace]
        if 'A_Pending' in control_flow:
            successful.append(i)
        else:
            unsuccessful.append(i)
    return successful, unsuccessful


# For workflow activities, concatenate the lifecycle to the activity itself
def adjust_lifecycles(log):
    for trace in log:
        for event in trace:
            act = event['concept:name']
            if act.startswith('W'):
                lifecycle = event['lifecycle:transition']
                new_act = act + '|' + lifecycle
                event['concept:name'] = new_act
    return log


def preprocess_bpic_2017():
    my_path = r'C:\Users\bakullari\Documents\hlem_framework\event_logs\BPI-Challenge-2017.xes'
    cache_path = my_path.replace('.xes', '.pickle')
    if os.path.isfile(cache_path):
        with open(cache_path, 'rb') as f:
            log = pickle.load(f)
    else:
        log = pm4py.read_xes(my_path)
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)

    # filter incomplete cases
    log = filter_incomplete_cases(log)

    # attach lifecycle info to activity label
    log = adjust_lifecycles(log)

    # partition of cases using outcome
    successful, not_successful = partition_on_outcome(log)

    # partition of cases using throughput
    class_under_10, class_10_to_30, class_over_30 = partition_on_throughput(log)

    # determine res_selected which excludes User_1
    resources = get_res_selection(log)

    return log, resources, successful, not_successful, class_under_10, class_10_to_30, class_over_30


# ============= The following parameters are fixed for all experiments =============
traffic_type = 'High'  # means we are interested in particularly "high" values
selected_f_list = hlem_overlap.DEFAULT_HLF  # set to ['enter', 'exit', 'handover', 'workload', 'batch', 'delay']
#selected_f_list =['exit', 'enter', 'handover', 'workload']
res_info = True  # assuming the log has resource info, otherwise 'handover' and 'workload' won't be computed
act_selection = 'all'  # we want to consider all activities and thus all segments in the process
# this has no effect on the result
seg_method = 'df'  # means the steps we consider (event pairs traversing a segment) are the directly follows event pairs
type_based = True  # if True, runtime is smaller

# ============= The following parameters can/should be configured =============
p = 0.9  # the extremity threshold, the values above the pth percentile will generate a hle
co_thresh = 0.5  # the case overlap threshold for connecting hle pairs
co_path_thresh = 0.5  # the case overlap threshold for a path of hle, should not be higher than co_thresh
only_maximal_paths = True  # by default, consider only maximal paths
path_freq = 10  # if 0 consider all hla paths, if a number > 0, consider only hla paths with frequency count above
frame = 'days'  # how to partition the time space onto windows


def run_eval():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    # my_path = r'C:\Users\bakullari\Documents\hlem_framework\event_logs\running-example.xes'
    # cache_path = my_path.replace('.xes', '.pickle')
    # if os.path.isfile(cache_path):
    #     with open(cache_path, 'rb') as f:
    #         log = pickle.load(f)
    # else:
    #     log = pm4py.read_xes(my_path)
    #     with open(cache_path, 'wb') as f:
    #         pickle.dump(log, f)

    log, res_selection, success_cases, non_success_cases, cases_under_10, cases_10_to_30, cases_over_30 = \
        preprocess_bpic_2017()

    hla_paths_dict = hlem_overlap.paths_and_cases_with_overlap(input_log=log, frame=frame,
                                                                                          traffic_type=traffic_type,
                                                                                          selected_f_list=selected_f_list,
                                                                                          p=p, co_thresh=0.5,
                                                                                          co_path_thresh=0.5,
                                                                                          res_info=True,
                                                                                          only_maximal_paths=only_maximal_paths,
                                                                                          path_frequency=path_freq,
                                                                                          act_selection=act_selection,
                                                                                          res_selection=res_selection,
                                                                                          seg_method=seg_method,
                                                                                          type_based=type_based)


    cf_dict = case_participation.get_cf_dict(log)
    postprocess.gather_statistics(hla_paths_dict, cf_dict, p, co_thresh)

    logging.info('Exited successfully.')


if __name__ == '__main__':
    run_eval()


