import extractor
import component
import frames
import congestion_state as cs_state
import hle_generation as hle_gen
import cascade
import hl_log_po
import hl_log_flat
from datetime import timezone


def transform_log_to_hl_log_width(log, number, traffic_type, selected_f_list, p=0.8, relative_congestion=True,
                                  connection_thresh=0.5, res_info=False, freq=0.8, only_component=False,
                                  act_selection='all', res_selection='all', seg_method="df", flatten=True):

    try:
        res_info is False and ('wl' in selected_f_list or 'do' in selected_f_list or 'todo' in selected_f_list or
                               len(res_selection) > 0)
    except ValueError:
        print("Set resource_info to True if you want to analyze resources and their features")

    print('Computing segments, components and their proximity')
    # first: create event dictionary, event_pairs, trig and release dicts, proximity measure
    event_dict = extractor.event_dict(log, res_info)
    pairs, trig, rel = extractor.trig_rel_dicts(log, seg_method)
    A_set, R_set, S_set = component.components(event_dict, pairs, res_info)
    comp_type_dic = component.comp_type_dict(A_set, R_set, S_set)
    proximity_abs = component.proximity(event_dict, pairs, trig, rel, res_info)
    proximity = component.normalize_weights(proximity_abs)

    print('Computing frames, partitioning events into frames')
    width = frames.get_width_from_number(event_dict, number)
    bucketId_borders_dict = frames.bucket_window_dict_by_width(event_dict, width)
    bucketId_eventList_dict, id_frame_mapping = frames.bucket_id_list_dict_by_width(event_dict, width)

    print('Computing cs across all time windows')
    cs_complete = cs_state.cs(A_set, R_set, S_set, event_dict, trig, bucketId_borders_dict, id_frame_mapping, pairs,
                              res_info, act_selection, res_selection)
    cs_all = cs_state.cs_feature_selection(cs_complete, selected_f_list)

    print('Generating high-level events')
    hle_all_windows, freq_dict = hle_gen.hle_all_windows(traffic_type, cs_all, comp_type_dic, p, relative_congestion)
    print('Computing cascades')
    G = cascade.hle_graph_weighted(hle_all_windows, proximity, comp_type_dic, connection_thresh)
    cascade_dict = cascade.cascade_id(G)
    print('Projecting on frequent high-level activities')
    hla_list_filtered = hle_gen.filter_hla(freq_dict, freq)

    # first_ts = log[0][0]['time:timestamp']
    # tz_info = first_ts.tzinfo
    tz_info = timezone.utc
    print('Generating high-level log and dataframe')
    if flatten:
        hl_log, hl_log_df = hl_log_flat.generate_hl_log(bucketId_borders_dict, hle_all_windows, cascade_dict, tz_info,
                                                        hla_list_filtered, only_component)
    else:
        hl_log, hl_log_df = hl_log_po.generate_hl_log(bucketId_borders_dict, hle_all_windows, cascade_dict, tz_info,
                                                      hla_list_filtered, only_component)

    return hl_log, hl_log_df
