def components(event_dic, pairs_list, res_info):

    A = set([event_dic[ev]['act'] for ev in event_dic.keys()])
    S = set([(event_dic[i]['act'], event_dic[j]['act']) for (i, j) in pairs_list])
    R = []
    if res_info:
        R = set([event_dic[ev]['res'] for ev in event_dic.keys()])

    return A, R, S


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
