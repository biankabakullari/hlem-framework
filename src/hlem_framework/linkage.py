import itertools as it
from collections import defaultdict
import component


def global_counts(event_dic, pairs_list, trig_dic, rel_dic, res_info):
    """
    This function returns:
    -   a_counts: a dictionary where a_counts[a] for any activity a is the total number of events executing a
    -   r_counts: a dictionary where r_counts[r] for any resource r is the total number of events executed by r
    -   s_counts: a dictionary where s_counts[s] for any segment s is the total number of steps traversing s
    -   aa_counts: a dictionary where aa_counts[(a1, a2)] for any activity pair a1, a2 is the total number of steps
     traversing (a1, a2), aa_counts also has keys for activity pairs that are not in the segment set
    -   ar_counts: a dictionary where ar_counts[(a, r)] for any activity a and resource r is the total number of events
    where a is executed by r
    -   rs_counts: a dictionary where rs_counts[(r, s)] for any resource r and segment s is the total number of steps
    traversing s (either entering or exiting s) when r executes an event

    """
    activity_set, resource_set, segment_set = component.components(event_dic, pairs_list, res_info)

    a_counts = dict.fromkeys(activity_set, 0)
    s_counts = dict.fromkeys(segment_set, 0)

    aa_counts = dict.fromkeys(list(it.product(activity_set, activity_set)), 0)
    ss_counts = dict.fromkeys(list(it.product(segment_set, segment_set)), 0)

    r_counts = dict.fromkeys(resource_set, 0)
    rr_counts = dict.fromkeys(list(it.product(resource_set, resource_set)), 0)
    ar_counts = dict.fromkeys(list(it.product(activity_set, resource_set)), 0)
    rs_counts = dict.fromkeys(list(it.product(resource_set, segment_set)), 0)

    for ev in event_dic.keys():
        a = event_dic[ev]['act']
        a_counts[a] += 1
        trigger_set = trig_dic[ev]
        release_set = rel_dic[ev]
        if len(trigger_set) and len(release_set):
            # note: if one of them is zero and the trace has two events, that possible event pair is handled when
            # considering act_pairs below
            for e_last in release_set:
                for e_next in trigger_set:
                    a_previous = event_dic[e_last]['act']
                    a_next = event_dic[e_next]['act']
                    s1 = (a_previous, a)
                    s2 = (a, a_next)
                    ss_counts[(s1, s2)] += 1

        if res_info and len(resource_set):
            r = event_dic[ev]['res']
            r_counts[r] += 1
            ar_counts[(a, r)] += 1

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

    return a_counts, r_counts, s_counts, aa_counts, rr_counts, ss_counts, ar_counts, rs_counts


def link(event_dic, pairs_list, trig_dic, rel_dic, res_info):
    """
    Given the absolute counts of any component pair, the link values are computed and normalized.
    For details: refer to the pg. 5 of the paper.
    """
    a_counts, r_counts, s_counts, aa_counts, rr_counts, ss_counts, ar_counts, rs_counts = \
        global_counts(event_dic, pairs_list, trig_dic, rel_dic, res_info)
    link_dict = {'aa': dict.fromkeys(list(aa_counts.keys()), 0.0),
                 'rr': dict.fromkeys(list(rr_counts.keys()), 0.0),
                 'ss': dict.fromkeys(list(ss_counts.keys()), 0.0),
                 'ar': dict.fromkeys(list(ar_counts.keys()), 0.0),
                 'as': defaultdict(lambda: 0),
                 'rs': dict.fromkeys(list(rs_counts.keys()), 0.0)
                 }

    # the link value for each activity pair
    for a1, a2 in link_dict['aa'].keys():
        a1_freq, a2_freq = a_counts[a1], a_counts[a2]
        a1_a2_freq, a2_a1_freq = aa_counts[(a1, a2)], aa_counts[(a2, a1)]
        # val = 0.5 * (a1_a2_freq / a1_freq) + 0.5 * (a2_a1_freq / a2_freq)
        val = max((a1_a2_freq / a1_freq), (a2_a1_freq / a2_freq))
        link_dict['aa'][(a1, a2)] = val

    # the link value for each segment pair
    for s1, s2 in link_dict['ss'].keys():
        s1_freq, s2_freq = s_counts[s1], s_counts[s2]
        s1_s2_freq, s2_s1_freq = ss_counts[(s1, s2)], ss_counts[(s2, s1)]
        # val = 0.5 * (s1_s2_freq / s1_freq) + 0.5 * (s2_s1_freq / s2_freq)
        val = max((s1_s2_freq / s1_freq), (s2_s1_freq / s2_freq))
        link_dict['ss'][(s1, s2)] = val

    # the link value for each activity-segment pair
    for a in a_counts.keys():
        for s in aa_counts.keys():
            if a in s:
                a_freq, s_freq = a_counts[a], aa_counts[s]
                val = s_freq / a_freq
            else:
                val = 0
            link_dict['as'][(a, s)] = val

    if res_info:
        # the link value for each resource pair
        for r1, r2 in link_dict['rr'].keys():
            r1_freq, r2_freq = r_counts[r1], r_counts[r2]
            r1_r2_freq, r2_r1_freq = rr_counts[(r1, r2)], rr_counts[(r2, r1)]
            # val = 0.5 * (r1_r2_freq / r1_freq) + 0.5 * (r2_r1_freq / r2_freq)
            val = max((r1_r2_freq / r1_freq), (r2_r1_freq / r2_freq))
            link_dict['rr'][(r1, r2)] = val

        # the link value for each activity-resource pair
        for a, r in link_dict['ar'].keys():
            a_freq, r_freq = a_counts[a], r_counts[r]
            a_r_freq = ar_counts[(a, r)]
            val = max((a_r_freq / r_freq), (a_r_freq / a_freq))
            link_dict['ar'][(a, r)] = val

        # the link value for each resource-segment pair
        for r, s in link_dict['rs'].keys():
            r_freq, s_freq = r_counts[r], s_counts[s]
            r_s_freq = rs_counts[(r, s)]
            val = max((r_s_freq / r_freq), (r_s_freq / s_freq))
            link_dict['rs'][(r, s)] = val

    # mapping all values onto [0,1]
    for xy_string in link_dict.keys():
        xy_string_values = list(link_dict[xy_string].values())
        if len(xy_string_values):
            xy_max = max(xy_string_values)
        else: # if no resource information, then xy_string_values for e.g. xy = 'rr' will be an empty list
            xy_max = 0
        if xy_max > 0:
            # true iff there is at least one component pair with link > 0, otherwise no need to normalize
            link_dict[xy_string] = {pair: link_dict[xy_string][pair] / xy_max for pair in link_dict[xy_string].keys()}
        yx_pairs = {(pair[1], pair[0]): link_dict[xy_string][pair] for pair in link_dict[xy_string].keys()}
        link_dict[xy_string].update(yx_pairs)
    return link_dict


def spread_weights(all_weights):
    """
    this function distributes all the normalized weights uniformly over the [0,1] spectrum,
    e.g.: value multiset [0, 0.1, 0.15, 0.2, 1] would be "spread" to [0, 0.25, 0.5, 0.75, 1]

    :param all_weights: a list of weights from [0,1], some may be 0
    :return: a dictionary where each key is an old weight, and the value is the new weight
    """
    #print(all_weights)
    mapping = dict.fromkeys(all_weights, 0.0)
    positive_weights = [w for w in all_weights if w > 0]
    if len(positive_weights):
        unique_positive_weights = set(positive_weights)
        normalizer = len(unique_positive_weights)
        positive_weights_sorted = sorted(list(unique_positive_weights))

        for index, w in enumerate(positive_weights_sorted):
            pos = index + 1  # add 1 so that the first positive weight with pos=0 does not get mapped to 0
            mapping[w] = pos / normalizer

    return mapping


def spread_link(link_dict):
    link_dict_spread = link_dict

    for xy_string in link_dict.keys():
        xy_links = [link_dict[xy_string][pair] for pair in link_dict[xy_string].keys()]
        xy_links_spread = spread_weights(xy_links)
        for pair in link_dict[xy_string].keys():
            old_link = link_dict[xy_string][pair]
            link_dict_spread[xy_string][pair] = xy_links_spread[old_link]

    return link_dict_spread


# def weight_threshold(multiset, p):
#     try:
#         50 < p < 100
#     except ValueError:
#         print('Weight threshold must be within (50,100)')
#
#     # we consider only non-zero evaluations for determining the p-th percentile
#     multiset_no_zeros = list(filter(lambda m: m != 0, multiset))
#
#     if len(multiset_no_zeros) == 0:
#         return 10 ** 10
#     else:
#         return np.percentile(multiset_no_zeros, p)
