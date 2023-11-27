def components(event_dic, steps_list, res_info):
    """

    :param event_dic:
    :param steps_list:
    :param res_info:
    :return:
    """
    """
    returns:
     -  the activity set (from the 'act' attribute values in the event_doc),
     -  the resource set (from the 'res' attribute values in the event_doc, empty set if res_info=False),
     -  the segments set (activity pairs from the event pairs in the steps_list)
    """
    A = set([event_dic[ev]['act'] for ev in event_dic.keys()])
    S = set([(event_dic[i]['act'], event_dic[j]['act']) for (i, j) in steps_list])
    R = set([])
    if res_info:
        R = set([event_dic[ev]['res'] for ev in event_dic.keys()])

    return A, R, S


def comp_type_dict(activity_set, resource_set, segment_set):
    """
    returns a dictionary, where each component (each activity, each resource and each segment) is a key, and its value
    shows what kind of component it is (an activity, a resource or a segment)
    """
    comp_type = {}

    for a in activity_set:
        comp_type[a] = 'activity'

    if len(resource_set) > 0:
        for r in resource_set:
            comp_type[r] = 'resource'

    for s in segment_set:
        comp_type[s] = 'segment'

    return comp_type
