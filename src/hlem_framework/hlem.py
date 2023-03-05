import preprocess
import component
import linkage
import frames
import instance
import eval
import hle_generation as hle_gen
import correlation_by_linkage as corr_link
import hl_log
from datetime import timezone

DEFAULT_HLF = frozenset(['exit', 'enter', 'wt', 'cross', 'busy', 'do', 'todo', 'exec', 'to-exec', 'handover', 'workload'])


def transform_log_to_hl_log_width(log, frame, traffic_type, selected_f_list, p, connection_thresh, res_info, freq,
                                  only_component, act_selection, res_selection, seg_method, flatten):

    try:
        res_info is False and ('busy' in selected_f_list or 'do' in selected_f_list or 'todo' in selected_f_list or
                               len(res_selection) > 0)
    except ValueError:
        print("Set resource_info to True if you want to analyze resources and their features")

    logging.info('Computing steps and components.')
    # first: create event dictionary, event_pairs, trig and release dicts, components, and link values
    event_dict = preprocess.event_dict(log, res_info)
    steps, trigger, release = preprocess.trig_rel_dicts(log, seg_method)
    set_A, set_R, set_S = component.components(event_dict, steps, res_info)
    component_types_dic = component.comp_type_dict(set_A, set_R, set_S)

    logging.info('Computing frames, partitioning events into frames.')
    ids_sorted = frames.sorted_ids_by_ts(event_dict)
    w_events_list, id_window_mapping = frames.framing(event_dict, frame)
    window_borders_dict = frames.windows_borders_dict(event_dict, frame, ids_sorted)

    logging.info('Evaluating the high-level features across all time windows.')
    instance_hlf_w_complete, instance_hlf_w_pair_complete = instance.instances_hlf(set_A, set_R, set_S, event_dict,
                                                                                   trigger, window_borders_dict,
                                                                                   id_window_mapping, steps, res_info,
                                                                                   act_selection, res_selection,
                                                                                   selected_f_list)
    evaluation_by_theta_complete = eval.evaluation(instance_hlf_w_complete, instance_hlf_w_pair_complete, event_dict,
                                                   id_window_mapping, window_borders_dict)

    logging.info('Generating high-level events.')
    hle_all_dic, hle_all_by_theta, freq_dict = hle_gen.hle_all(traffic_type, evaluation_by_theta_complete,
                                                               component_types_dic, p)


    logging.info('Correlating high-level events into high-level cases.')
    # correlation in hlem using the link values: a value between 0 and 1 for each entity pair
    # note that the link values solely depend on the info in the log, the generated hle are not considered
    link_abs = linkage.link(event_dict, steps, trigger, release, res_info)
    link = linkage.spread_link(link_abs)
    G = corr_link.hle_graph_weighted(hle_all_by_theta, link, connection_thresh)
    cascade_dict = corr_link.cascade_id(G)
    logging.info('Projecting on frequent high-level activities')
    hla_list_filtered = hle_gen.filter_hla(freq_dict, freq)
    tz_info = timezone.utc
    logging.info('Generating high-level log and dataframe')
    hl_log_xes, hl_log_df = hl_log.generate_hl_log(window_borders_dict, hle_all_by_theta, cascade_dict, tz_info,
                                                   hla_list_filtered, only_component, flatten)

    return hl_log_xes, hl_log_df


#if __name__ == '__main__':
