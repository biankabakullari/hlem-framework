import itertools as it
from collections import defaultdict


def entity_occurrence_counts(event_dict, steps_list, all_A, all_S, all_R, trigger_dict, release_dict, res_info):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of (i,j) pairs, where i and j event identifiers of event pairs that constitute a step
    :param all_A: the set of all activities in the data (no filter applied)
    :param all_S: the set of all segments in the data (no filter applied)
    :param all_R: the set of all resources in the data (no filter applied)
    :param trigger_dict: a dictionary where each key,value pair i: [j1,...,jn] means that set (i,j1),...,(i,jn) are steps
    :param release_dict: a dictionary where each key,value pair i: [j1,...,jn] means that set (i,j1),...,(i,jn) are steps
    :param res_info: default is False, if True, resource information is collected
    :return:
    -   a_counts: a dictionary where a_counts[a] for any activity a is the total number of events executing a
    -   r_counts: a dictionary where r_counts[r] for any resource r is the total number of events executed by r
    -   s_counts: a dictionary where s_counts[s] for any segment s is the total number of steps traversing s
    -   aa_counts: a dictionary where aa_counts[(a1, a2)] for any activity pair a1, a2 is the total number of steps
     traversing (a1, a2), aa_counts also has keys for activity pairs that are not in the segment set
    -   ar_counts: a dictionary where ar_counts[(a, r)] for any activity a and resource r is the total number of events
    where a is executed by r
    -   rs_counts: a dictionary where rs_counts[(r, s)] for any resource r and segment s is the total number of steps
    traversing s (either entering or exiting s) when r executes an event underlying s
    """
    # Note how we compute the link values for all pairs of entities, NOT ONLY for the ones whose high-level events we
    # are interested in. This way, the extremity t

    a_counts = dict.fromkeys(all_A, 0)
    s_counts = dict.fromkeys(all_S, 0)

    aa_counts = dict.fromkeys(list(it.product(all_A, all_A)), 0)
    ss_counts = dict.fromkeys(list(it.product(all_S, all_S)), 0)

    r_counts = dict.fromkeys(all_R, 0)
    rr_counts = dict.fromkeys(list(it.product(all_R, all_R)), 0)
    ar_counts = dict.fromkeys(list(it.product(all_A, all_R)), 0)
    rs_counts = dict.fromkeys(list(it.product(all_R, all_S)), 0)

    for ev in event_dict.keys():
        a = event_dict[ev]['act']
        a_counts[a] += 1
        trigger_set = trigger_dict[ev]
        release_set = release_dict[ev]
        
        # if true, then the current event is not a start event and not an end event within its trace
        if len(trigger_set) and len(release_set):
            # note: if one of them is zero and the trace has two events, that one event pair is handled when
            # considering act_pairs below
            for e_previous in release_set:
                for e_next in trigger_set:
                    a_previous = event_dict[e_previous]['act']
                    a_next = event_dict[e_next]['act']
                    s1 = (a_previous, a)
                    s2 = (a, a_next)
                    ss_counts[(s1, s2)] += 1

        if res_info and len(all_R):
            r = event_dict[ev]['res']
            r_counts[r] += 1
            ar_counts[(a, r)] += 1

    all_act_pairs = [(event_dict[ei]['act'], event_dict[ej]['act']) for ei, ej in steps_list]
    #start_seg_indices = [index for index, (ei, ej) in enumerate(steps_list) if event_dict[ei]['is-start']]
    #end_seg_indices = [index for index, (ei, ej) in enumerate(steps_list) if event_dict[ej]['is-end']]

    if res_info and len(all_R):
        res_pairs = [(event_dict[ei]['res'], event_dict[ej]['res']) for ei, ej in steps_list]
    else:
        res_pairs = []

    for index, act_pair in enumerate(all_act_pairs):
        aa_counts[act_pair] += 1
        s_counts[act_pair] += 1
        if res_info and len(all_R):
            res_pair = res_pairs[index]
            rr_counts[res_pair] += 1

            rs_counts[(res_pair[0], act_pair)] += 1/2
            rs_counts[(res_pair[1], act_pair)] += 1/2

    return a_counts, r_counts, s_counts, aa_counts, rr_counts, ss_counts, ar_counts, rs_counts


def entity_pair_link(event_dict, steps_list, all_A, all_S, all_R, trigger_dict, release_dict, res_info):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of (i,j) pairs, where i and j event identifiers of event pairs that constitute a step
    :param all_A: all activities in the data
    :param all_S: all segments in the data
    :param all_R: all resources in the data
    :param trigger_dict: a dictionary where each key,value pair i: [j1,...,jn] means that set (i,j1),...,(i,jn) are steps
    :param release_dict: a dictionary where each key,value pair i: [j1,...,jn] means that set (i,j1),...,(i,jn) are steps
    :param res_info: default is False, if True, resource information is collected
    :return:
    A dict where link_dict['ar'][('request', 'Jane')]=link_dict['ar'][('Jane', 'request')] reflects the link between
    activity 'request' and resource 'Jane'
    Given the absolute counts of any entity pair, the link values are computed and normalized w.r.t. component pair.
    For details: refer to the pg. 5 of the "High-level Event Mining: A Framework" paper.
    """
    a_counts, r_counts, s_counts, aa_counts, rr_counts, ss_counts, ar_counts, rs_counts = \
        entity_occurrence_counts(event_dict, steps_list, all_A, all_S, all_R, trigger_dict, release_dict, res_info)
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
        val = max((a1_a2_freq / a1_freq), (a2_a1_freq / a2_freq))
        link_dict['aa'][(a1, a2)] = val

    # the link value for each segment pair
    for s1, s2 in link_dict['ss'].keys():
        s1_freq, s2_freq = s_counts[s1], s_counts[s2]
        s1_s2_freq, s2_s1_freq = ss_counts[(s1, s2)], ss_counts[(s2, s1)]
        # NOTE: this may mean that usually (a,b) is followed by (b,c), but you may later connect a (b,c) hle to \
        # an (a,b) hle (backward propagation)
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
            #NOTE: this may mean that usually r1 hands over to r2, but you may later connect an r2 hle to an r1 hle \
            # (backward propagation)
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

    # mapping all link values onto [0,1]
    for xy_string in link_dict.keys():
        xy_string_values = list(link_dict[xy_string].values())
        if len(xy_string_values):
            xy_max = max(xy_string_values)
        else:  # if no resource information, then xy_string_values for e.g. xy = 'rr' will be an empty list
            xy_max = 0
        if xy_max > 0:
            # true iff there is at least one component pair with link > 0, otherwise no need to normalize
            link_dict[xy_string] = {pair: link_dict[xy_string][pair] / xy_max for pair in link_dict[xy_string].keys()}

        # the following makes sure that there are also keys (r, a), (s, a), and (s, r) with the same value as (a, r), \
        # (a, s), and (s, r) as the link value is symmetrical
        yx_pairs = {(pair[1], pair[0]): link_dict[xy_string][pair] for pair in link_dict[xy_string].keys()}
        link_dict[xy_string].update(yx_pairs)
    return link_dict


def uniform_spread_weights(all_weights):
    """
    :param all_weights: a list of weights from [0,1], some may be 0
    :return: a dictionary where each key is an old weight, and the value is the new weight.
    This function distributes all the normalized weights uniformly over the [0,1] spectrum,
    e.g.: value multiset [0, 0.1, 0.15, 0.2, 1] would be "spread" to [0, 0.25, 0.5, 0.75, 1]
    """

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


def uniform_spread_link(link_dict):
    """
    :param link_dict: A dict where link_dict['ar'][('request', 'Jane')]=link_dict['ar'][('Jane', 'request')]
    reflects the link between activity 'request' and resource 'Jane'
    :return: The link_dict values between entity pairs, such that they are uniformly spread between 0 and 1.
    """
    link_dict_spread = link_dict

    for xy_string in link_dict.keys():
        xy_links = [link_dict[xy_string][pair] for pair in link_dict[xy_string].keys()]
        xy_links_spread = uniform_spread_weights(xy_links)
        for pair in link_dict[xy_string].keys():
            old_link = link_dict[xy_string][pair]
            link_dict_spread[xy_string][pair] = xy_links_spread[old_link]

    return link_dict_spread
