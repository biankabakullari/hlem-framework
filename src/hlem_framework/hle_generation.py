import numpy as np
from collections import defaultdict
import logging


def high_threshold(multiset, p):
    try:
        50 < p < 100
    except ValueError:
        print('p must be within (50,100)')

    multiset_no_zeros = list(filter(lambda m: m != 0, multiset))

    if len(multiset_no_zeros) == 0:
        return 10 ** 10
    else:
        return np.percentile(multiset_no_zeros, p)


def low_threshold(multiset, p):
    try:
        50 < p < 100
    except ValueError:
        print('p must be within (50,100)')

    multiset_no_zeros = list(filter(lambda m: m != 0, multiset))

    if len(multiset_no_zeros) == 0:
        return 0
    else:
        return np.percentile(multiset_no_zeros, 100 - p)


def get_thresholds(multiset, p):
    low = low_threshold(multiset, p)
    high = high_threshold(multiset, p)

    return low, high


def class_value(value, low, high):
    if value < low:
        return "Low"
    elif value > high:
        return "High"
    else:
        return "Normal"


def get_eval_list_per_hlf(eval_complete):
    """
    Computes for each hlf (e.g. exec-a, busy-Jane, cross-ab) the list of all measured values across the different
    windows
    """
    hlf_values = {}

    for theta in eval_complete.keys():
        eval_theta = eval_complete[theta]
        for hlf in eval_theta.keys():
            val = eval_theta[hlf]
            if hlf in hlf_values.keys():
                hlf_values[hlf].append(val)
            else:
                hlf_values[hlf] = [val]

    return hlf_values


def get_hlf_thresholds(eval_all, p):
    """
    Computes for each list of values recorded for the same hlf (e.g. exec-a, wl-Jane, progress-ab) the p-th percentile.
    """
    all_hlf_thresh = {}
    all_hlf_values = get_eval_list_per_hlf(eval_all)

    for hlf in all_hlf_values.keys():
        f_type = hlf[0]
        this_hlf_values = all_hlf_values[hlf]
        if f_type == 'delay':
            p_hard_set = 0.7
            # note that each entry is a pair (# windows distance, # steps)
            window_deltas = [entry[0] for entry in this_hlf_values]
            low, high = get_thresholds(window_deltas, p_hard_set)
        else:
            low, high = get_thresholds(this_hlf_values, p)
        all_hlf_thresh[hlf] = (low, high)

    return all_hlf_thresh


# traffic_type must be [High], [Low], or [High, Low]
def hle_theta_by_hlf(event_dict, traffic_type, theta, eval_complete, instance_hlf_complete, comp_type_dict,
                     all_hlf_thresholds, frequencies_last, id_counter, last_case_set_dic):

    hle_theta_dic = {}
    eval_at_theta = eval_complete[theta]
    # i = 0  # the id of the generated hle within the theta
    for hlf in eval_at_theta.keys():
        hlf_value = eval_at_theta[hlf]  # this is a pair (window delta, no. instances) for f-type = 'delay'
        f_type = hlf[0]  # e.g. busy
        entity = hlf[1]  # e.g. Jane
        entity_comp = comp_type_dict[entity]
        low, high = all_hlf_thresholds[hlf]
        if f_type == 'delay':
            class_v = class_value(hlf_value[0], low, high)
        else:
            class_v = class_value(hlf_value, low, high)
        if class_v in traffic_type:
            if f_type == 'delay':
                no_windows = hlf_value[0]
                no_instances = hlf_value[1]
                if no_instances >= 10:
                    hle = {'f-type': f_type, 'entity': entity, 'class': class_v, 'value': (no_windows, no_instances),
                           'component': entity_comp, 'theta': theta}
                else:
                    continue
            # hle = (f_type, entity, class_v, hlf_value, entity_comp)
            # hle_theta.append(hle)
            else:
                hle = {'f-type': f_type, 'entity': entity, 'class': class_v, 'value': hlf_value,
                       'component': entity_comp, 'theta': theta}
            hle_theta_dic[id_counter] = hle
            instance_list = instance_hlf_complete[theta][hlf]
            last_case_set_dic[id_counter] = get_case_set(instance_list, event_dict)
            # only the first three entries (f_type, entity, traffic type) determine the high-level activity
            hla = (f_type, entity, class_v)
            frequencies_last[hla] += 1
            id_counter += 1
    return hle_theta_dic, frequencies_last, id_counter, last_case_set_dic


def get_eval_list_per_type(eval_complete, comp_type_dict):
    """
    Computes for each f-type (e.g. exec, busy, cross) the list of all measured values across the different entities
    (e.g. all 'exec-a' values for any a from the activity set)
    """
    eval_list_type = {}  # e.g. feature_values_type[('activity', 'exec')] = [...]

    for theta in eval_complete.keys():
        eval_theta = eval_complete[theta]
        for hlf in eval_theta.keys():
            f_type = hlf[0]
            entity = hlf[1]
            component = comp_type_dict[entity]
            type_component_pair = (f_type, component)
            val = eval_theta[hlf]
            if type_component_pair in eval_list_type.keys():
                eval_list_type[type_component_pair].append(val)
            else:
                eval_list_type[type_component_pair] = [val]

    return eval_list_type


def get_type_thresholds(eval_all, comp_type_dict, p):
    """
    Computes for each measure (e.g. exec, wl, progress) the p-th percentile.
    The threshold is determined for each measure: e.g. the 80th percentile over all 'exec-a' values measured over all a
    from the activity set).
    """
    all_types_thresh = {}
    all_types_values = get_eval_list_per_type(eval_all, comp_type_dict)

    for type_comp_pair in all_types_values.keys():
        f_type = type_comp_pair[0]
        type_comp_values = all_types_values[type_comp_pair]
        if f_type == 'delay':
            p_hard_set = 0.7
            # note that each entry is a pair (# windows distance, # steps)
            window_deltas = [entry[0] for entry in type_comp_values]
            low, high = get_thresholds(window_deltas, p_hard_set)
        else:
            low, high = get_thresholds(type_comp_values, p)
        all_types_thresh[type_comp_pair] = (low, high)

    return all_types_thresh


def get_case_set(instance_list, event_dict):
    case_list = []
    for entry in instance_list:
        if len(entry) == 1:
            case_id = event_dict[entry]['case']
        else:  # each entry is an event pair
            event = entry[0]  # both events have same case id, so it doesn't matter which one we take
            case_id = event_dict[event]['case']
        case_list.append(case_id)
    return set(case_list)


# traffic_type must be [High], [Low], or [High, Low]
def hle_theta_by_type(event_dict, traffic_type, theta, eval_complete, instance_hlf_complete, comp_type_dict,
                      all_types_thresholds, frequencies_last, id_counter, last_case_set_dic):

    hle_theta_dic = {}
    eval_at_theta = eval_complete[theta]
    # i = 0  # the id of the generated hle within the theta
    for hlf in eval_at_theta.keys():
        hlf_value = eval_at_theta[hlf]  # this is a pair (window delta, no. instances) for f-type = 'delay'
        f_type = hlf[0]  # e.g. busy
        entity = hlf[1]  # e.g. Jane
        entity_comp = comp_type_dict[entity]
        type_comp_pair = (f_type, entity_comp)
        low, high = all_types_thresholds[type_comp_pair]
        if f_type == 'delay':
            class_v = class_value(hlf_value[0], low, high)
        else:
            class_v = class_value(hlf_value, low, high)

        if class_v in traffic_type:
            if f_type == 'delay':
                no_windows = hlf_value[0]
                no_instances = hlf_value[1]
                if no_instances >= 10:  # only considering bundles that are big enough
                    hle = {'f-type': f_type, 'entity': entity, 'class': class_v, 'value': (no_windows, no_instances),
                           'component': entity_comp, 'theta': theta}
                else:
                    continue
            # hle = (f_type, entity, class_v, hlf_value, entity_comp)
            # hle_theta.append(hle)
            else:
                hle = {'f-type': f_type, 'entity': entity, 'class': class_v, 'value': hlf_value,
                       'component': entity_comp, 'theta': theta}
            hle_theta_dic[id_counter] = hle
            instance_list = instance_hlf_complete[theta][hlf]
            last_case_set_dic[id_counter] = get_case_set(instance_list, event_dict)
            # only the first three entries (f_type, entity, traffic type) determine the high-level activity
            hla = (f_type, entity, class_v)
            frequencies_last[hla] += 1
            id_counter += 1
    return hle_theta_dic, frequencies_last, id_counter, last_case_set_dic


def hle_all(event_dict, traffic_type, eval_complete, instance_hlf_complete, comp_type_dict, p, type_based):

    hle_all_dic = {}
    hle_all_by_theta = {}
    last_case_set_dic = {}

    hla_frequencies = defaultdict(lambda: 0)
    last_freq = hla_frequencies
    id_counter = 0

    if type_based:
        type_thresholds = get_type_thresholds(eval_complete, comp_type_dict, p)
        for theta in eval_complete.keys():
            hle_theta_dic, last_freq_updated, id_counter, case_set_dic = hle_theta_by_type(event_dict, traffic_type,
                                                                                           theta, eval_complete,
                                                                                           instance_hlf_complete,
                                                                                           comp_type_dict,
                                                                                           type_thresholds, last_freq,
                                                                                           id_counter,
                                                                                           last_case_set_dic)
            last_case_set_dic = case_set_dic
            last_freq = last_freq_updated
            hle_all_by_theta[theta] = hle_theta_dic
            for hle_id in hle_theta_dic.keys():
                hle = hle_theta_dic[hle_id]
                hle_all_dic[hle_id] = hle
    else:
        hlf_thresholds = get_hlf_thresholds(eval_complete, p)
        for theta in eval_complete.keys():
            hle_theta_dic, last_freq_updated, id_counter, case_set_dic = hle_theta_by_hlf(event_dict, traffic_type,
                                                                                          theta, eval_complete,
                                                                                          instance_hlf_complete,
                                                                                          comp_type_dict,
                                                                                          hlf_thresholds, last_freq,
                                                                                          id_counter, last_case_set_dic)
            last_case_set_dic = case_set_dic
            last_freq = last_freq_updated
            hle_all_by_theta[theta] = hle_theta_dic
            for hle_id in hle_theta_dic.keys():
                hle = hle_theta_dic[hle_id]
                hle_all_dic[hle_id] = hle
    no_hle = len(hle_all_dic.keys())
    logging.info('We detected ' + str(no_hle) + ' high-level events.')
    return hle_all_dic, hle_all_by_theta, last_freq, last_case_set_dic


def filter_hla(freq_dict, freq_thresh):
    hla_filtered = []
    freq_values = [freq_dict[triple] for triple in freq_dict.keys()]

    #were = sum([freq_dict[hla] for hla in freq_dict.keys()])
    #print("Were: ", were)

    if 0 < freq_thresh < 1:  # a freq_thresh=0.8 requests selecting only the 20% most frequent high-level activities
        percentile = freq_thresh * 100
        number = high_threshold(freq_values, percentile)
        #sizes_new = 0
        for hla in freq_dict.keys():
            if freq_dict[hla] >= number:
                hla_filtered.append(hla)
                #sizes_new += freq_dict[hla]

        # print("Are:", sizes_new)
        return hla_filtered

    elif freq_thresh > 1:  # a freq_thresh=7 requests selecting the seven most frequent high-level activities
        most_frequent = sorted(freq_dict.keys(), key=lambda x: freq_dict[x])[:freq_thresh]
        #sizes_new = sum([freq_dict[hla] for hla in most_frequent])

        #print("Are:", sizes_new)
        return most_frequent

    else:  # freq_thresh <= 0 means no filtering required
        hla_unfiltered = freq_dict.keys()
        return hla_unfiltered
