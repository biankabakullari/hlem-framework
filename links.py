import itertools as it
import numpy as np
import component


def statistics(event_dic, pairs_list, trig_dic, rel_dic, res_info):
    activity_set, resource_set, segment_set = component.components(event_dic, pairs_list, res_info)

    a_counts = {a: 0 for a in activity_set}
    s_counts = {s: 0 for s in segment_set}

    aa_counts = {(a1, a2): 0 for (a1, a2) in it.product(activity_set, activity_set)}
    ss_counts = {(s1, s2): 0 for (s1, s2) in it.product(segment_set, segment_set)}
    as_counts = {(a, s): 0 for (a, s) in it.product(activity_set, segment_set)}

    r_counts = {0: 0}
    rr_counts = {(0, 0): 0}
    ar_counts = {(0, 0): 0}
    rs_counts = {(0, (0, 0)): 0}

    for ev in event_dic.keys():
        ev_act = event_dic[ev]['act']
        a_counts[ev_act] += 1
        trig_set = trig_dic[ev]
        rel_set = rel_dic[ev]
        if len(trig_set) > 0 and len(rel_set) > 0:
            for e_last in rel_set:
                for e_next in trig_set:
                    act_last = event_dic[e_last]['act']
                    act_next = event_dic[e_next]['act']
                    s1 = (act_last, ev_act)
                    s2 = (ev_act, act_next)
                    ss_counts[(s1, s2)] += 1

        if res_info and len(resource_set):
            r_counts = {r: 0 for r in resource_set}
            rr_counts = {(r1, r2): 0 for (r1, r2) in it.product(resource_set, resource_set)}
            ar_counts = {(a, r): 0 for (a, r) in it.product(activity_set, resource_set)}
            rs_counts = {(r, s): 0 for (r, s) in it.product(resource_set, segment_set)}

            ev_res = event_dic[ev]['res']
            r_counts[ev_res] += 1
            ar_counts[(ev_act, ev_res)] += 1

    act_pairs = [(event_dic[ei]['act'], event_dic[ej]['act']) for ei, ej in pairs_list]
    for x, act_pair in enumerate(act_pairs):
        aa_counts[act_pair] += 1
        s_counts[act_pair] += 1
        if res_info and len(resource_set):
            res_pairs = [(event_dic[ei]['res'], event_dic[ej]['res']) for ei, ej in pairs_list]
            res_pair = res_pairs[x]
            rr_counts[res_pair] += 1
            rs_counts[(res_pair[0], act_pair)] += 1 / 2
            rs_counts[(res_pair[1], act_pair)] += 1 / 2

    return a_counts, r_counts, s_counts, aa_counts, rr_counts, ss_counts, ar_counts, as_counts, rs_counts


def link(event_dic, pairs_list, trig_dic, rel_dic, res_info):

    a_counts, r_counts, s_counts, aa_counts, rr_counts, ss_counts, ar_counts, as_counts, rs_counts = \
        statistics(event_dic, pairs_list, trig_dic, rel_dic, res_info)
    link_dic = {'aa': {(a1, a2): 0 for (a1, a2) in aa_counts.keys()},
                'rr': {(r1, r2): 0 for (r1, r2) in rr_counts.keys()},
                'ss': {(s1, s2): 0 for (s1, s2) in ss_counts.keys()},
                'ar': {(a, r): 0 for (a, r) in ar_counts.keys()},
                'as': {(a, s): 0 for (a, s) in as_counts.keys()},
                'rs': {(r, s): 0 for (r, s) in rs_counts.keys()}}

    # the link value for each activity pair
    for a1, a2 in link_dic['aa'].keys():
        a1_freq, a2_freq = a_counts[a1], a_counts[a2]
        a1_a2_freq, a2_a1_freq = aa_counts[(a1, a2)], aa_counts[(a2, a1)]
        # val = 0.5 * (a1_a2_freq / a1_freq) + 0.5 * (a2_a1_freq / a2_freq)
        val = max((a1_a2_freq / a1_freq), (a2_a1_freq / a2_freq))
        link_dic['aa'][(a1, a2)] = val

    # the link value for each segment pair
    for s1, s2 in link_dic['ss'].keys():
        s1_freq, s2_freq = ss_counts[s1], s_counts[s2]
        s1_s2_freq, s2_s1_freq = ss_counts[(s1, s2)], ss_counts[(s2, s1)]
        # val = 0.5 * (s1_s2_freq / s1_freq) + 0.5 * (s2_s1_freq / s2_freq)
        val = max((s1_s2_freq / s1_freq), (s2_s1_freq / s2_freq))
        link_dic['ss'][(s1, s2)] = val

    # the link value for each activity-segment pair
    for a, s in link_dic['as'].keys():
        if a in s:
            a_freq, s_freq = a_counts[a], s_counts[s]
            val = s_freq / a_freq
            link_dic['as'][(a, s)] = val

        # else: if a not in s, then the link value will stay 0 as initialized

    if res_info:
        # the link value for each resource pair
        for r1, r2 in link_dic['rr'].keys():
            r1_freq, r2_freq = r_counts[r1], r_counts[r2]
            r1_r2_freq, r2_r1_freq = rr_counts[(r1, r2)], rr_counts[(r2, r1)]
            # val = 0.5 * (r1_r2_freq / r1_freq) + 0.5 * (r2_r1_freq / r2_freq)
            val = max((r1_r2_freq / r1_freq), (r2_r1_freq / r2_freq))
            link_dic['rr'][(r1, r2)] = val

        # the link value for each activity-resource pair
        for a, r in link_dic['ar'].keys():
            a_freq, r_freq = a_counts[a], r_counts[r]
            a_r_freq = ar_counts[(a, r)]
            val = max((a_r_freq / r_freq), (a_r_freq / a_freq))
            link_dic['ar'][(a, r)] = val

        # the link value for each resource-segment pair
        for r, s in link_dic['rs'].keys():
            r_freq, s_freq = r_counts[r], s_counts[s]
            r_s_freq = rs_counts[(r, s)]
            val = max((r_s_freq / r_freq), (r_s_freq / s_freq))
            link_dic['rs'][(r, s)] = val

    # mapping all values onto [0,1]
    for xy_string in link_dic.keys():
        xy_max = max(list(link_dic[xy_string].values()))
        if xy_max > 0:
            # true iff there is at least one component pair with link > 0, otherwise no need to normalize
            link_dic[xy_string] = {pair: link_dic[xy_string][pair] / xy_max for pair in link_dic[xy_string].keys()}
        yx_pairs = {(pair[1], pair[0]): link_dic[xy_string][pair] for pair in link_dic[xy_string].keys()}
        link_dic[xy_string].update(yx_pairs)
    return link_dic


def remap_weights(weight_list, weight_list_no_zeros):
    unique_weights = set(weight_list_no_zeros)
    normalizer = len(unique_weights)
    weights_no_zeros_sorted = sorted(list(unique_weights))
    mapping = {w: 0 for w in weight_list}

    for index, w in enumerate(weights_no_zeros_sorted):
        pos = index + 1
        mapping[w] = pos / normalizer

    return mapping


# the function below distributes all the normalized weights uniformly over the [0,1] spectrum
# e.g.: value multiset [0, 0.1, 0.15, 0.2, 1] would be "spread" to [0, 0.25, 0.5, 0.75, 1]
def spread_weights(proximity_dict):
    proximity_normalized = proximity_dict

    for xy_comp in proximity_dict.keys():
        xy_weights = [proximity_dict[xy_comp][pair] for pair in proximity_dict[xy_comp].keys()]
        xy_weights_no_zeros = [n for n in xy_weights if n > 0]
        if len(xy_weights_no_zeros) > 0:
            xy_remapped = remap_weights(xy_weights, xy_weights_no_zeros)
            for pair in proximity_dict[xy_comp].keys():
                weight = proximity_dict[xy_comp][pair]
                proximity_normalized[xy_comp][pair] = xy_remapped[weight]
    return proximity_normalized


# delete?
def weight_threshold(multiset, p):
    try:
        50 < p < 100
    except ValueError:
        print('Weight threshold must be within (50,100)')

    # we consider only non-zero evaluations for determining the p-th percentile
    multiset_no_zeros = list(filter(lambda m: m != 0, multiset))

    if len(multiset_no_zeros) == 0:
        return 10 ** 10
    else:
        return np.percentile(multiset_no_zeros, p)