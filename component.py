import itertools as it
import numpy as np


def weight_threshold(multiset, p):
    try:
        50 < p < 100
    except ValueError:
        print('Weight threshold must be within (50,100)')

    multiset_no_zeros = list(filter(lambda m: m != 0, multiset))

    if len(multiset_no_zeros) == 0:
        return 10**10
    else:
        return np.percentile(multiset_no_zeros, p)


def components(event_dic, pairs_list, res_info):

    A = set([event_dic[ev]['act'] for ev in event_dic.keys()])
    S = set([(event_dic[i]['act'], event_dic[j]['act']) for (i, j) in pairs_list])
    R = []
    if res_info:
        R = set([event_dic[ev]['res'] for ev in event_dic.keys()])

    return A, R, S


def statistics(event_dic, pairs_list, trig_dic, rel_dic, res_info):

    activity_set, resource_set, segment_set = components(event_dic, pairs_list, res_info)

    A_counts = {a: 0 for a in activity_set}
    S_counts = {s: 0 for s in segment_set}

    AA_counts = {(a1, a2): 0 for (a1, a2) in it.product(activity_set, activity_set)}
    SS_counts = {(s1, s2): 0 for (s1, s2) in it.product(segment_set, segment_set)}
    AS_counts = {(a, s): 0 for (a, s) in it.product(activity_set, segment_set)}

    R_counts = {0: 0}
    RR_counts = {(0, 0): 0}
    AR_counts = {(0, 0): 0}
    RS_counts = {(0, (0, 0)): 0}

    if res_info and len(resource_set):
        R_counts = {r: 0 for r in resource_set}
        RR_counts = {(r1, r2): 0 for (r1, r2) in it.product(resource_set, resource_set)}
        AR_counts = {(a, r): 0 for (a, r) in it.product(activity_set, resource_set)}
        RS_counts = {(r, s): 0 for (r, s) in it.product(resource_set, segment_set)}

        for ev in event_dic.keys():

            ev_act = event_dic[ev]['act']
            ev_res = event_dic[ev]['res']
            A_counts[ev_act] += 1
            R_counts[ev_res] += 1
            AR_counts[(ev_act, ev_res)] += 1

            trig_set = trig_dic[ev]
            rel_set = rel_dic[ev]
            if len(trig_set) > 0 and len(rel_set) > 0:
                for e_last in rel_set:
                    for e_next in trig_set:
                        act_last = event_dic[e_last]['act']
                        act_next = event_dic[e_next]['act']
                        s1 = (act_last, ev_act)
                        s2 = (ev_act, act_next)
                        SS_counts[(s1, s2)] += 1

        act_pairs = [(event_dic[ei]['act'], event_dic[ej]['act']) for ei, ej in pairs_list]
        res_pairs = [(event_dic[ei]['res'], event_dic[ej]['res']) for ei, ej in pairs_list]

        for x, act_pair in enumerate(act_pairs):
            res_pair = res_pairs[x]
            AA_counts[act_pair] += 1
            RR_counts[res_pair] += 1
            S_counts[act_pair] += 1
            RS_counts[(res_pair[0], act_pair)] += 1/2
            RS_counts[(res_pair[1], act_pair)] += 1/2

    else:  # resource_set is empty
        for ev in event_dic.keys():
            ev_act = event_dic[ev]['act']
            A_counts[ev_act] += 1

            trig_set = trig_dic[ev]
            rel_set = rel_dic[ev]
            if len(trig_set) > 0 and len(rel_set) > 0:
                for e_last in rel_set:
                    for e_next in trig_set:
                        act_last = event_dic[e_last]['act']
                        act_next = event_dic[e_next]['act']
                        s1 = (act_last, ev_act)
                        s2 = (ev_act, act_next)
                        SS_counts[(s1, s2)] += 1

        act_pairs = [(event_dic[ei]['act'], event_dic[ej]['act']) for ei, ej in pairs_list]

        for act_pair in act_pairs:
            AA_counts[act_pair] += 1
            S_counts[act_pair] += 1

    return A_counts, R_counts, S_counts, AA_counts, RR_counts, SS_counts, AR_counts, AS_counts, RS_counts


def proximity(event_dic, pairs_list, trig_dic, rel_dic, res_info):

    A_counts, R_counts, S_counts, AA_counts, RR_counts, SS_counts, AR_counts, AS_counts, RS_counts = statistics(
                                                                            event_dic, pairs_list, trig_dic, rel_dic,
                                                                            res_info)

    proximity_dic = {'aa': {(a1, a2): 0 for (a1, a2) in AA_counts.keys()},
                     'rr': {(r1, r2): 0 for (r1, r2) in RR_counts.keys()},
                     'ss': {(s1, s2): 0 for (s1, s2) in SS_counts.keys()},
                     'ar': {(a, r): 0 for (a, r) in AR_counts.keys()},
                     'as': {(a, s): 0 for (a, s) in AS_counts.keys()},
                     'rs': {(r, s): 0 for (r, s) in RS_counts.keys()}}
    proximity_max = {'aa': 0, 'rr': 0, 'ss': 0, 'ar': 0, 'as': 0, 'rs': 0}

    for a1, a2 in proximity_dic['aa'].keys():
        a1_freq, a2_freq = A_counts[a1], A_counts[a2]
        a1_a2_freq, a2_a1_freq = AA_counts[(a1, a2)], AA_counts[(a2, a1)]
        # val = 0.5 * (a1_a2_freq / a1_freq) + 0.5 * (a2_a1_freq / a2_freq)
        val = max((a1_a2_freq / a1_freq), (a2_a1_freq / a2_freq))
        proximity_dic['aa'][(a1, a2)] = val
        if val > proximity_max['aa']:
            proximity_max['aa'] = val

    for s1, s2 in proximity_dic['ss'].keys():
        s1_freq, s2_freq = S_counts[s1], S_counts[s2]
        s1_s2_freq, s2_s1_freq = SS_counts[(s1, s2)], SS_counts[(s2, s1)]
        # val = 0.5 * (s1_s2_freq / s1_freq) + 0.5 * (s2_s1_freq / s2_freq)
        val = max((s1_s2_freq / s1_freq), (s2_s1_freq / s2_freq))
        proximity_dic['ss'][(s1, s2)] = val
        if val > proximity_max['ss']:
            proximity_max['ss'] = val

    for a, s in proximity_dic['as'].keys():
        if a in s:
            a_freq, s_freq = A_counts[a], S_counts[s]
            val = s_freq / a_freq
            proximity_dic['as'][(a, s)] = val
            if val > proximity_max['as']:
                proximity_max['as'] = val

    if res_info:
        for r1, r2 in proximity_dic['rr'].keys():
            r1_freq, r2_freq = R_counts[r1], R_counts[r2]
            r1_r2_freq, r2_r1_freq = RR_counts[(r1, r2)], RR_counts[(r2, r1)]
            # val = 0.5 * (r1_r2_freq / r1_freq) + 0.5 * (r2_r1_freq / r2_freq)
            val = max((r1_r2_freq / r1_freq), (r2_r1_freq / r2_freq))
            proximity_dic['rr'][(r1, r2)] = val
            if val > proximity_max['rr']:
                proximity_max['rr'] = val

        for a, r in proximity_dic['ar'].keys():
            a_freq, r_freq = A_counts[a], R_counts[r]
            a_r_freq = AR_counts[(a, r)]
            val = max((a_r_freq / r_freq), (a_r_freq / a_freq))
            proximity_dic['ar'][(a, r)] = val
            if val > proximity_max['ar']:
                proximity_max['ar'] = val

        for r, s in proximity_dic['rs'].keys():
            r_freq, s_freq = R_counts[r], S_counts[s]
            r_s_freq = RS_counts[(r, s)]

            val = max((r_s_freq / r_freq), (r_s_freq / s_freq))
            proximity_dic['rs'][(r, s)] = val
            if val > proximity_max['rs']:
                proximity_max['rs'] = val

    for xy_comp in proximity_dic.keys():
        xy_max = proximity_max[xy_comp]
        if xy_max > 0:
            proximity_dic[xy_comp] = {pair: proximity_dic[xy_comp][pair] / xy_max for pair in
                                      proximity_dic[xy_comp].keys()}
        yx_pairs = {(pair[1], pair[0]): proximity_dic[xy_comp][pair] for pair in proximity_dic[xy_comp].keys()}
        proximity_dic[xy_comp].update(yx_pairs)
    return proximity_dic


def remap_weights(weight_list, weight_list_no_zeros):

    unique_weights = set(weight_list_no_zeros)
    normalizer = len(unique_weights)
    weights_no_zeros_sorted = sorted(list(unique_weights))
    mapping = {w: 0 for w in weight_list}

    for index, w in enumerate(weights_no_zeros_sorted):
            pos = index + 1
            mapping[w] = pos / normalizer

    return mapping


def normalize_weights(proximity_dict):

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


def comp_type_dict(activity_set, resource_set, segment_set):

    comp_type = {}

    for a in activity_set:
        comp_type[a] = 'activity'

    if len(resource_set) > 0:
        for r in resource_set:
            comp_type[r] = 'resource'

    for s in segment_set:
        comp_type[s] = 'segment'

    return comp_type
