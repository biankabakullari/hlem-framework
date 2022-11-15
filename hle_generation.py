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
        return "N"


def get_all_feature_values(eval_all_aggr):
    feature_values = {}

    for w in eval_all_aggr.keys():
        eval_w_values = eval_all_aggr[w]
        for f_comp_pair in eval_w_values.keys():
            v = eval_w_values[f_comp_pair]
            if f_comp_pair in feature_values.keys():
                feature_values[f_comp_pair].append(v)
            else:
                feature_values[f_comp_pair] = []

    return feature_values


def get_all_feature_values_per_type(eval_all_aggr, comp_type_dict):

    feature_values_type = {}  # e.g. feature_values_type[('activity', 'exec')] = [...]

    for w in eval_all_aggr.keys():
        eval_w_values = eval_all_aggr[w]
        for f_comp_pair in eval_w_values.keys():
            f = f_comp_pair[0]
            comp = f_comp_pair[1]
            entity_type = comp_type_dict[comp]
            type_f_pair = (entity_type, f)
            v = eval_w_values[f_comp_pair]
            if type_f_pair in feature_values_type.keys():
                feature_values_type[type_f_pair].append(v)
            else:
                feature_values_type[type_f_pair] = []

    return feature_values_type


def get_all_feature_thresholds(eval_all, comp_type_dict, p, relative_congestion):
    all_feature_thresh = {}
    # all_values = get_all_feature_values(cs_all)
    all_values = get_all_feature_values_per_type(eval_all, comp_type_dict)

    if relative_congestion:  # look at congestion per type and feature
        for type_f_pair in all_values.keys():
            type_f_values = all_values[type_f_pair]
            low, high = get_thresholds(type_f_values, p)
            all_feature_thresh[type_f_pair] = (low, high)

    else:
        all_all = []
        for type_f_pair in all_values.keys():
            f_comp_values = all_values[type_f_pair]
            all_all.extend(f_comp_values)
        low, high = get_thresholds(all_all, p)
        for type_f_pair in all_values.keys():
            all_feature_thresh[type_f_pair] = (low, high)

    return all_feature_thresh


# traffic_type must be [High], [Low], or [High, Low]
def hle_window(traffic_type, window, eval_all_aggr, comp_type_dict, all_feature_thresholds, frequencies_last):

    hle_w = []
    window_cs_aggr = eval_all_aggr[window]
    for f_comp_pair in window_cs_aggr.keys():
        f_comp_value = window_cs_aggr[f_comp_pair]
        f = f_comp_pair[0]
        comp = f_comp_pair[1]
        comp_type = comp_type_dict[comp]
        type_f_pair = (comp_type, f)
        low, high = all_feature_thresholds[type_f_pair]
        class_v = class_value(f_comp_value, low, high)
        if class_v in traffic_type:
            hlf = (f, comp, class_v, f_comp_value, comp_type)
            hle_w.append(hlf)
            frequencies_last[hlf[:3]] += 1

    return hle_w, frequencies_last


def hle_all_windows(traffic_type, eval_all, comp_type_dict, p, relative_congestion):
    hle_all = {}
    hla_frequencies = defaultdict(lambda: 0)
    all_feature_thresholds = get_all_feature_thresholds(eval_all, comp_type_dict, p, relative_congestion)
    last_freq = hla_frequencies

    for w in eval_all.keys():
        hle_all[w], last_freq_updated = hle_window(traffic_type, w, eval_all, comp_type_dict, all_feature_thresholds,
                                                   last_freq)
        last_freq = last_freq_updated

    return hle_all, last_freq


def filter_hla(freq_dict, freq_thresh):
    hla_filtered = []
    freq_values = [freq_dict[triple] for triple in freq_dict.keys()]

    were = sum([freq_dict[hla] for hla in freq_dict.keys()])
    print("Were: ", were)

    if 0 < freq_thresh < 1:
        percentile = freq_thresh * 100
        number = high_threshold(freq_values, percentile)
        sizes_new = 0
        for hla in freq_dict.keys():
            if freq_dict[hla] >= number:
                hla_filtered.append(hla)
                sizes_new += freq_dict[hla]

        print("Are:", sizes_new)
        return hla_filtered

    elif freq_thresh > 1:
        most_frequent = sorted(freq_dict.keys(), key=lambda x: freq_dict[x])[:freq_thresh]
        sizes_new = sum([freq_dict[hla] for hla in most_frequent])

        print("Are:", sizes_new)
        return most_frequent

    else:
        hla_unfiltered = freq_dict.keys()
        return hla_unfiltered
