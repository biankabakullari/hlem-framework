import os
import math
import pickle
from datetime import datetime
import logging
import pm4py
import preprocess
import component
import frames
import instance
import eval
import hle_generation as hle_gen
import overlap
import case_participation
import correlation_by_overlap as corr_overlap
import high_level_paths as paths


DEFAULT_HLF = frozenset(['exit', 'enter', 'handover', 'workload', 'batch', 'delay'])


def paths_and_cases_with_overlap(input_log, frame, traffic_type, selected_f_list, p, co_thresh, co_path_thresh,
                                 res_info, only_maximal_paths, path_frequency, act_selection, res_selection,
                                 seg_method, type_based):

    logging.info('Computing steps.')
    # first: create event dictionary, event_pairs, trig and release dicts, components, and link values
    event_dict = preprocess.event_dict(input_log, res_info)
    steps, trigger, release = preprocess.trig_rel_dicts(input_log, seg_method)
    set_A, set_R, set_S = component.components(event_dict, steps, res_info)
    component_types_dic = component.comp_type_dict(set_A, set_R, set_S)

    logging.info('Computing windows, partitioning events into windows.')
    ids_sorted = frames.sorted_ids_by_ts(event_dict)
    w_events_list, id_window_mapping = frames.framing(event_dict, frame)
    window_borders_dict = frames.windows_borders_dict(event_dict, frame, ids_sorted)

    if isinstance(frame, int):
        logging.info('There are ' + str(len(window_borders_dict.keys())) + ' windows. Each window has width of ' +
                     str(frame) + 'seconds.')
    else:
        logging.info('There are ' + str(len(window_borders_dict.keys())) + ' ' + str(frame) + str('.'))

    logging.info('Evaluating the high-level event types across all time windows.')
    instance_hlf_w_complete, instance_hlf_w_pair_complete, instance_all_complete = instance.instances_hlf(set_A, set_R,
                                                                                                          set_S,
                                                                                                          event_dict,
                                                                                                          trigger,
                                                                                                          window_borders_dict,
                                                                                                          id_window_mapping,
                                                                                                          steps,
                                                                                                          res_info,
                                                                                                          act_selection,
                                                                                                          res_selection,
                                                                                                          selected_f_list)

    evaluation_by_theta_complete = eval.evaluation(instance_hlf_w_complete, instance_hlf_w_pair_complete, event_dict,
                                                   id_window_mapping, window_borders_dict)

    logging.info('Detecting high-level events and the involved cases using ' + str(p*100) + 'th percentile as extremity '
                                                                                     'threshold.')
    hle_all_dic, hle_all_by_theta, freq_dict, case_set_dic = hle_gen.hle_all(event_dict, traffic_type,
                                                                             evaluation_by_theta_complete,
                                                                             instance_all_complete, component_types_dic,
                                                                             p, type_based)

    logging.info('Computing overlap and connecting the hle.')
    spread_dic = overlap.spread_dict(hle_all_dic, instance_all_complete, id_window_mapping)
    G = corr_overlap.hle_graph(hle_all_dic, spread_dic, case_set_dic, co_thresh)

    filename = f'prepaths_{datetime.now().strftime("%d_%m_%Y %H_%M_%S")}.pickle'
    with open(filename, 'wb') as f:
        pickle.dump((G, case_set_dic, co_thresh, co_path_thresh), f)

    hle_paths = paths.hle_co_paths(G, case_set_dic, co_thresh, co_path_thresh, only_maximal_paths)
    # logging.info('Computing maximal paths and filtering them by frequency.')
    # if only_maximal_paths:
    #     # TODO: pickling no longer needed
    #     filename = f'maximal_paths_{datetime.now().strftime("%d_%m_%Y %H_%M_%S")}.pickle'
    #     with open(filename, 'wb') as f:
    #         pickle.dump(hle_paths, f)
    #     hle_paths = paths.get_maximal_paths(hle_paths)

    hle_paths_cases = case_participation.get_hle_paths_cases(hle_paths, case_set_dic)
    print('No. hle paths: ' + str(len(hle_paths_cases)))
    logging.info('Projecting hle paths to hla paths and merging the case sets.')
    hla_paths_dict = paths.hla_co_paths(hle_all_dic, hle_paths, hle_paths_cases)

    number_hla_paths = len(hla_paths_dict.keys())
    print('No. hla paths (projections): ' + str(number_hla_paths))
    if path_frequency > 0:
        hla_paths_dict = {
            hla_seq: (freq, cases) for hla_seq, (freq, cases) in hla_paths_dict.items() if freq >= path_frequency
        }
    logging.info("Method terminated")

    return hla_paths_dict


if __name__ == '__main__':
    my_path = r'C:\Users\bakullari\Documents\hlem_framework\event_logs\BPI-Challenge-2017.xes'
    cache_path = my_path.replace('.xes', '.pickle')
    if os.path.isfile(cache_path):
        with open(cache_path, 'rb') as f:
            log = pickle.load(f)
    else:
        log = pm4py.read_xes(my_path)
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)

    no_events = sum([len(trace) for trace in log])
    ts_first = log[0][0]['time:timestamp']
    last_trace = log[len(log) - 1]
    ts_last = log[len(log) - 1][len(last_trace) - 1]['time:timestamp']
    # seconds_total = (ts_last-ts_first).total_seconds()
    # no_windows = 6 * (seconds_total / 3600)  # used in the evaluation of the simulated log
    no_windows = math.ceil(math.sqrt(no_events))
    time_unit = 'days'
    paths_and_cases_with_overlap(input_log=log, frame=time_unit, traffic_type='High', selected_f_list=DEFAULT_HLF,
                                 p=0.9, co_thresh=0.5, co_path_thresh=0.5, res_info=True, only_maximal_paths=True,
                                 path_frequency=10, act_selection='all', res_selection='all', seg_method='df',
                                 type_based=True)
