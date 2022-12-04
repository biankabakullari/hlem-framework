import preprocess
import component
import linkage
import frames
import feature_eval
import hle_generation as hle_gen
import correlation
import hl_log
from datetime import timezone

DEFAULT_HLF = frozenset(['exec', 'enter', 'wt', 'wl'])


def transform_log_to_hl_log_width(log, number, traffic_type, selected_f_list, p, connection_thresh, res_info, freq,
                                  only_component, act_selection, res_selection, seg_method, flatten):

    try:
        res_info is False and ('wl' in selected_f_list or 'do' in selected_f_list or 'todo' in selected_f_list or
                               len(res_selection) > 0)
    except ValueError:
        print("Set resource_info to True if you want to analyze resources and their features")

    print('Computing steps, components and their link values.')
    # first: create event dictionary, event_pairs, trig and release dicts, components, and link values
    event_dict = preprocess.event_dict(log, res_info)
    steps, trigger, release = preprocess.trig_rel_dicts(log, seg_method)
    set_A, set_R, set_S = component.components(event_dict, steps, res_info)
    component_types_dic = component.comp_type_dict(set_A, set_R, set_S)
    link_abs = linkage.link(event_dict, steps, trigger, release, res_info)
    link = linkage.spread_link(link_abs)

    print('Computing frames, partitioning events into frames.')
    window_size = frames.get_window_size(event_dict, number)
    window_borders_dict = frames.window_borders_dict(event_dict, window_size)
    w_events_list, id_window_mapping = frames.window_events_dict(event_dict, window_size)

    print('Evaluating the high-level features across all time windows.')
    hlf_eval_complete = feature_eval.eval_hlf(set_A, set_R, set_S, event_dict, trigger, window_borders_dict,
                                              id_window_mapping, steps, res_info, act_selection, res_selection)
    hlf_eval_all = feature_eval.eval_hlf_selection(hlf_eval_complete, selected_f_list)

    print('Generating high-level events.')
    hle_all_windows, freq_dict = hle_gen.hle_all_windows(traffic_type, hlf_eval_all, component_types_dic, p)

    print('Correlating high-level events into high-level cases.')
    G = correlation.hle_graph_weighted(hle_all_windows, link, component_types_dic, connection_thresh)
    cascade_dict = correlation.cascade_id(G)
    print('Projecting on frequent high-level activities')
    hla_list_filtered = hle_gen.filter_hla(freq_dict, freq)

    # first_ts = log[0][0]['time:timestamp']
    # tz_info = first_ts.tzinfo
    tz_info = timezone.utc
    print('Generating high-level log and dataframe')
    hl_log_xes, hl_log_df = hl_log.generate_hl_log(window_borders_dict, hle_all_windows, cascade_dict, tz_info,
                                                   hla_list_filtered, only_component, flatten)

    return hl_log_xes, hl_log_df
