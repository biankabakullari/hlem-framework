# we don't compute a dictionary for all hle pairs, as we only check place overlap for a subset of hle pairs
def place_overlap(hle_all_dic, hle_id_1, hle_id_2):
    hle_1 = hle_all_dic[hle_id_1]
    hle_2 = hle_all_dic[hle_id_2]
    comp_1 = hle_1['component']
    comp_2 = hle_2['component']

    if comp_1 == 'segment' and comp_2 == 'segment':
        entity_1 = hle_1['entity']
        entity_2 = hle_2['entity']
        return entity_1[1] == entity_2[0]
    else:
        return False


def spread_dict(hle_all_dic, instance_hlf_complete, id_window_mapping):

    spread_dic = {}

    for hle_id in hle_all_dic.keys():
        hle = hle_all_dic[hle_id]
        f_type = hle['f-type']
        entity = hle['entity']
        theta = hle['theta']
        comp = hle['component']

        if comp == 'segment' and f_type in ['enter', 'exit', 'workload', 'handover', 'batch', 'delay']:
            if not isinstance(theta, int) and len(theta) == 2:  # hle refers to a bundle
                start_spread = [theta[0]]  # enter window of bundle
                end_spread = [theta[1]]  # exit window of bundle
            else:  # theta is single window
                hlf = (f_type, entity)
                id_pairs = instance_hlf_complete[theta][hlf]
                if f_type in ['exit', 'workload', 'handover']:
                    start_spread = sorted(list(set([id_window_mapping[id_pair[0]] for id_pair in id_pairs])))
                    end_spread = [theta]
                else:  # f_type is 'enter'
                    start_spread = [theta]
                    end_spread = sorted(list(set([id_window_mapping[id_pair[1]] for id_pair in id_pairs])))
        else:
            start_spread = [theta]
            end_spread = [theta]

        start_spread_first_window = start_spread[0]
        start_spread_last_window = start_spread[len(start_spread)-1]
        end_spread_first_window = end_spread[0]
        end_spread_last_window = end_spread[len(end_spread)-1]

        spread_dic[hle_id] = {'start-spread-first': start_spread_first_window,
                              'start-spread-last': start_spread_last_window,
                              'end-spread-first': end_spread_first_window,
                              'end-spread-last': end_spread_last_window}

    return spread_dic


def time_overlap(hle_id_1, hle_id_2, spread_dic):
    start_set_1, end_set_1 = spread_dic[hle_id_1]['start-spread'], spread_dic[hle_id_1]['end-spread']
    start_set_2, end_set_2 = spread_dic[hle_id_2]['start-spread'], spread_dic[hle_id_2]['end-spread']
    return set(end_set_1).issubset(set(start_set_2)) or set(start_set_2).issubset(set(end_set_1))


# returns True if the intersection is not empty
def time_overlap2(hle_id_1, hle_id_2, spread_dic):
    start_set_1, end_set_1 = spread_dic[hle_id_1]['start-spread'], spread_dic[hle_id_1]['end-spread']
    start_set_2, end_set_2 = spread_dic[hle_id_2]['start-spread'], spread_dic[hle_id_2]['end-spread']
    intersection_1 = set(end_set_1).intersection(set(start_set_2))
    intersection_2 = set(start_set_2).intersection(set(end_set_1))
    return len(intersection_1) > 0 or len(intersection_2) > 0


# we don't compute a dictionary for all hle pairs, as we only check case overlap for a subset of hle pairs
def case_overlap(hle_id_1, hle_id_2, case_set_dic):
    cases1 = case_set_dic[hle_id_1]
    cases2 = case_set_dic[hle_id_2]

    intersection = cases1.intersection(cases2)
    union = cases1.union(cases2)
    overlap_ratio = len(intersection) / len(union)

    return intersection, overlap_ratio





