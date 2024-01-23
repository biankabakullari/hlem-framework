from typing import Literal
from collections import Counter

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


def get_activities_that_cover(event_dict, coverage):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param coverage: a number in (0,1] for the event coverage based on activity value frequency
    :return: for coverage e.g., 0.6 it returns the most frequent activities that cover 60% of the events, it DOES NOT
    project the event data onto those events
    """
    # Calculate the total number of events
    total_events = len(event_dict)

    # Count the occurrences of each "act" value
    act_counts = Counter(event['act'] for event in event_dict.values())

    # Sort "act" values by frequency in descending order
    sorted_acts = sorted(act_counts.keys(), key=lambda x: act_counts[x], reverse=True)

    # Define the coverage percentage (e.g.,0.8 means that you want to know which activities make up >=80% of the events)
    coverage_percentage = 100*coverage

    # Initialize variables to track coverage
    events_covered = 0
    selected_acts = []

    # Iterate through the sorted "act" values and accumulate them until reaching the threshold
    for act in sorted_acts:
        events_covered += act_counts[act]
        selected_acts.append(act)
        if (events_covered / total_events) * 100 >= coverage_percentage:
            break

    return selected_acts


def get_resources_that_cover(event_dict, coverage):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param coverage: a number in (0,1] for the event coverage based on resource value frequency
    :return: for coverage e.g., 0.6 it returns the most frequent resources that cover 60% of the events, it DOES NOT
    project the event data onto those events
    """
    # Calculate the total number of events
    total_events = len(event_dict)

    # Count the occurrences of each "act" value
    res_counts = Counter(event['res'] for event in event_dict.values())

    # Sort "act" values by frequency in descending order
    sorted_res = sorted(res_counts.keys(), key=lambda x: res_counts[x], reverse=True)

    # Define the coverage percentage (e.g.,0.8 means that you want to know which resources make up >=80% of the events)
    coverage_percentage = 100*coverage

    # Initialize variables to track coverage
    events_covered = 0
    selected_res = []

    # Iterate through the sorted "act" values and accumulate them until reaching the threshold
    for res in sorted_res:
        events_covered += res_counts[res]
        selected_res.append(res)
        if (events_covered / total_events) * 100 >= coverage_percentage:
            break
    return selected_res


def get_segments_that_cover(event_dict, steps_list, coverage):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of (i,j) pairs, where i and j event identifiers of event pairs that constitute a step
    :param coverage: a number in (0,1] for the steps (think of directly-follows events, arcs in the DFG) coverage based
    on their underlying activity value pair (segment) frequency
    :return: for coverage e.g., 0.6 it returns the most frequent activity pairs (segments) that cover 60% of the steps,
    it DOES NOT project the event data onto those steps
    """
    # Create a dictionary to count activity pairs (segments)
    activity_pair_counts = Counter()

    # Count activity pairs based on the list of integer pairs
    for i, j in steps_list:
        act_x = event_dict[i]['act']
        act_y = event_dict[j]['act']
        activity_pair = (act_x, act_y)
        activity_pair_counts[activity_pair] += 1

    # Sort activity pairs by frequency in descending order
    sorted_activity_pairs = sorted(activity_pair_counts.items(), key=lambda pair: pair[1], reverse=True)

    # Define the coverage percentage (e.g.,0.8 means that you want to know which segments make up >=80% of the event
    # pairs)
    coverage_percentage = 100 * coverage

    # Initialize variables to track coverage
    pairs_covered = 0
    selected_activity_pairs = []

    # Iterate through the sorted activity pairs and accumulate them until reaching the threshold
    for activity_pair, count in sorted_activity_pairs:
        pairs_covered += count
        selected_activity_pairs.append(activity_pair)
        if (pairs_covered / len(steps_list)) * 100 >= coverage_percentage:
            break

    return selected_activity_pairs


def get_entities_for_analysis(event_dict, steps_list, res_info, act_selected, seg_selected, res_selected):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of (i,j) pairs, where i and j event identifiers of event pairs that constitute a step
    :param res_info: True or False (iff resource attribute considered for analysis)
    :param act_selected: A list of the activities of interest to do high-level event analysis, or 'all'
    :param seg_selected: the set of segments chosen for analysis
    :param res_selected: A list of the resources of interest to do high-level event analysis, or 'all'
    :return:
    -   A: the set of activities that qualify for analysis, determined by act_selected
    -   R: the set of resources that qualify for analysis, determined by res_selected
    -   S: the set of segments that qualify for analysis, determined by act_selected
        (those with both underlying activities are in A)
    """

    activity_set, segment_set, resource_set = get_entities(event_dict, steps_list, res_info)

    if act_selected == 'all':
        A = activity_set
    elif isinstance(act_selected, float):
        A = get_activities_that_cover(event_dict, act_selected)
    else:
        A = act_selected

    if seg_selected == 'all':
        S = segment_set
    elif isinstance(seg_selected, float):
        S = get_segments_that_cover(event_dict, steps_list, seg_selected)
    else:
        S = seg_selected

    R = res_selected
    if res_selected == 'all':
        R = resource_set
    elif isinstance(res_selected, float):
        R = get_resources_that_cover(event_dict, res_selected)

    return A, S, R


def comp_type_dict(activity_set, segment_set, resource_set):
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
