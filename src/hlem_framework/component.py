from typing import Literal


Component = Literal['activity', 'segment', 'resource']


def get_entities(event_dict, steps_list, res_info):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of (i,j) pairs, where i and j event identifiers of event pairs that constitute a step
    :param res_info: True or False (iff resource attribute considered for analysis)
    :return:
     -  all_activities: the activity set (from the 'act' attribute values in the event_dict),
     -  all_segments: the segment set (activity pairs from the event pairs in the steps_list),
     -  all_resources: the resource set (from the 'res' attribute values in the event_dict, empty set if res_info=False)
    """

    all_activities = set([event_dict[ev]['act'] for ev in event_dict.keys()])
    all_segments = set([(event_dict[i]['act'], event_dict[j]['act']) for (i, j) in steps_list])

    if res_info:
        all_resources = set([event_dict[ev]['res'] for ev in event_dict.keys()])
    else:
        all_resources = set()

    return all_activities, all_segments, all_resources


def comp_type_dict(activity_set, resource_set, segment_set):
    """
    :param activity_set: the set of all activity values in event log
    :param resource_set: the set of all resource values in event log
    :param segment_set: the set of all directly-follows activity pairs in event log
    :return: a dictionary {'a': 'activity', ..., 'r': 'resource, ..., 's': 'segment'} for any activity a in
    activity_set, resource r in resource_set and segment s in segment_set
    """

    comp_type = {}

    for a in activity_set:
        comp_type[a]: Component = 'activity'

    # will be skipped if resource_set is empty
    for r in resource_set:
        comp_type[r]: Component = 'resource'

    for s in segment_set:
        comp_type[s]: Component = 'segment'

    return comp_type


#TODO: make act_selected, res_selected and seg_selected possible as lists AND numbers
def get_entities_for_analysis(activity_set, resource_set, segment_set, act_selected, res_selected):
    """
    :param activity_set: the set of all activity values in event log
    :param resource_set: the set of all resource values in event log
    :param segment_set: the set of all directly-follows activity pairs in event log
    :param act_selected: A list of the activities of interest to do high-level event analysis, or 'all'.
    :param res_selected: A list of the resources of interest to do high-level event analysis, or 'all'.
    :return:
    -   A: the set of activities that qualify for analysis, determined by act_selected
    -   R: the set of resources that qualify for analysis, determined by res_selected
    -   S: the set of segments that qualify for analysis, determined by act_selected
        (those with both underlying activities are in A)
    """
    if act_selected == 'all':
        A = activity_set
        S = segment_set
    else:
        A = act_selected
        S = [(s1, s2) for (s1, s2) in segment_set if s1 in A and s2 in A]

    R = res_selected
    if res_selected == 'all':
        R = resource_set

    return A, S, R
