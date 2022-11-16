import numpy as np
from collections import defaultdict


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


def get_all_feature_values(eval_hlf_complete):
    feature_values = {}

    for w in eval_hlf_complete.keys():
        eval_w = eval_hlf_complete[w]
        for hlf in eval_w.keys():
            v = eval_w[hlf]
            if hlf in feature_values.keys():
                feature_values[hlf].append(v)
            else:
                feature_values[hlf] = [v]

    return feature_values


def get_eval_values_per_measure(eval_hlf_complete, comp_type_dict):
    """
    Computes for each measure (e.g. exec, wl, progress) the list of all measured values across the different entities
    (e.g. all 'exec-a' values for any a from the activity set)
    """
    eval_values_measure = {}  # e.g. feature_values_type[('activity', 'exec')] = [...]

    for w in eval_hlf_complete.keys():
        eval_w = eval_hlf_complete[w]
        for hlf in eval_w.keys():
            measure = hlf[0]
            entity = hlf[1]
            entity_type = comp_type_dict[entity]
            type_measure_pair = (entity_type, measure)
            v = eval_w[hlf]
            if type_measure_pair in eval_values_measure.keys():
                eval_values_measure[type_measure_pair].append(v)
            else:
                eval_values_measure[type_measure_pair] = [v]

    return eval_values_measure


def get_measure_thresholds(eval_all, comp_type_dict, p):
    """
    Computes for each measure (e.g. exec, wl, progress) the p-th percentile.
    The threshold is determined for each measure: e.g. the 80th percentile over all 'exec-a' values measured over all a
    from the activity set).
    """
    all_measures_thresh = {}
    all_measures_values = get_eval_values_per_measure(eval_all, comp_type_dict)

    for type_measure_pair in all_measures_values.keys():
        type_measure_values = all_measures_values[type_measure_pair]
        low, high = get_thresholds(type_measure_values, p)
        all_measures_thresh[type_measure_pair] = (low, high)

    return all_measures_thresh


# traffic_type must be [High], [Low], or [High, Low]
def hle_window(traffic_type, window, eval_hlf_all, comp_type_dict, all_measure_thresholds, frequencies_last):

    hle_w = []
    eval_hlf_window = eval_hlf_all[window]
    for hlf in eval_hlf_window.keys():
        hlf_value = eval_hlf_window[hlf]
        measure = hlf[0]  # e.g. wl
        entity = hlf[1]  # e.g. Jane
        entity_type = comp_type_dict[entity]
        type_measure_pair = (entity_type, measure)
        low, high = all_measure_thresholds[type_measure_pair]
        class_v = class_value(hlf_value, low, high)
        if class_v in traffic_type:
            hle = (measure, entity, class_v, hlf_value, entity_type)
            hle_w.append(hle)
            frequencies_last[hlf[:3]] += 1  # only the first three entries determine the high-level activity

    return hle_w, frequencies_last


def hle_all_windows(traffic_type, eval_hlf_all, comp_type_dict, p):
    hle_all = {}
    hla_frequencies = defaultdict(lambda: 0)
    all_measure_thresholds = get_measure_thresholds(eval_hlf_all, comp_type_dict, p)
    last_freq = hla_frequencies

    for w in eval_hlf_all.keys():
        hle_all[w], last_freq_updated = hle_window(traffic_type, w, eval_hlf_all, comp_type_dict,
                                                   all_measure_thresholds, last_freq)
        last_freq = last_freq_updated

    return hle_all, last_freq


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

    elif freq_thresh > 1: # a freq_thresh=7 requests selecting the seven most frequent high-level activities
        most_frequent = sorted(freq_dict.keys(), key=lambda x: freq_dict[x])[:freq_thresh]
        #sizes_new = sum([freq_dict[hla] for hla in most_frequent])

        #print("Are:", sizes_new)
        return most_frequent

    else:  # freq_thresh <= 0 means no filtering required
        hla_unfiltered = freq_dict.keys()
        return hla_unfiltered
