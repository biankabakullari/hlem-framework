from dataclasses import dataclass
import frames
from collections import Counter
from typing import Union


@dataclass
class ProjectLogArgs:
    activity_coverage: Union[int, float, str] = 'all'
    segment_coverage: Union[int, float, str] = 'all'

    def validate_coverage_combination(self):
        coverage_values = [
            self.activity_coverage,
            self.segment_coverage
        ]

        if coverage_values.count('all') == 0:
            raise ValueError("The log projection can be made based on one component only: activity or segment!")

    def __post_init__(self):
        self.validate_coverage_combination()


def event_dic_with_resource(log):
    """
    :param log: the event log
    :return: a dictionary where each key is a number uniquely identifying some event from the log, the value is a
    dictionary containing the attribute values for the case, activity, timestamp, resource. The value of 'single' is
    True iff the event is the only one recorded for the corresponding trace.
    """
    event_dic = {}
    pos = 0
    for trace_index, trace in enumerate(log):
        n = len(trace)
        is_single = n == 1
        case = trace_index

        for j in range(n):
            event = trace[j]
            act = event['concept:name']
            ts = event['time:timestamp']
            ts_seconds = frames.seconds_since_epoch(ts)
            # TODO: check if this still works with negative seconds
            assert ts_seconds > 0, 'Event happened before January 1st, 1970!'

            res = event['org:resource']
            if res == '':
                res = 'UNK'

            is_start = j == 0
            is_end = j == n - 1
            event_dic[pos + j] = {'case': case, 'act': act, 'ts': ts, 'ts-seconds': ts_seconds, 'res': res,
                                  'single': False, 'is-start': is_start, 'is-end': is_end}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dic_wo_resource(log):
    """
    :param log: the event log
    returns a dictionary where each key is a number uniquely identifying some event from the log, the value is a
    dictionary containing the attribute values for the activity, timestamp. The value of 'single' is True iff
    the event is the only one recorded for the corresponding trace
    """
    event_dic = {}
    pos = 0
    for i, trace in enumerate(log):
        n = len(trace)
        is_single = n == 1
        case = i
        for j in range(n):
            event = trace[j]
            act = event['concept:name']
            ts = event['time:timestamp']
            ts_seconds = frames.seconds_since_epoch(ts)
            # TODO: check if this still works with negative seconds
            assert ts_seconds > 0, 'Event happened before January 1st, 1970!'
            is_start = j == 0
            is_end = j == n-1
            event_dic[pos + j] = {'case': case, 'act': act, 'ts': ts, 'ts-seconds': ts_seconds, 'single': is_single,
                                  'is-start': is_start, 'is-end': is_end}

        pos += n

    return event_dic


def create_event_dict(log, res_info):
    """
    :param log: the event log
    :param res_info: default is False, if True, resource information is collected
    :return: a dictionary where each key is a unique identifier of an event (natural number), the value is a dict with
    each attribute as key and attribute value as value
    """
    if res_info:
        return event_dic_with_resource(log)
    else:
        return event_dic_wo_resource(log)


def data_projection_by_activity_percentage(event_dict, steps_list, coverage):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of pairs of numbers, each number uniquely identifies an event from the log, each pair
    constitutes a step
    :param coverage: a number in (0,1] for the event coverage based on activity value frequency
    :return: for coverage e.g., 0.6 it returns a filtered event dict containing only the 60% of events covered by most
    frequent activities (projects event data onto these events), and the steps that only contain filtered events
    """
    # Count the occurrences of each "act" value
    act_counts = Counter(event['act'] for event in event_dict.values())

    # Sort "act" values by frequency in descending order
    sorted_acts = sorted(act_counts.keys(), key=lambda x: act_counts[x], reverse=True)

    # number of events to keep
    num_events_to_keep = int(len(event_dict) * coverage)
    selected_events = set()
    for act in sorted_acts:
        # all events of next most frequent activity are added
        for ev in event_dict.keys():
            if event_dict[ev]['act'] == act:
                selected_events.add(ev)
        # stop adding events of the next activity as soon as enough events are added w.r.t. the desired coverage
        if len(selected_events) >= num_events_to_keep:
            break

    filtered_event_dict = {ev_id: event_data for ev_id, event_data in event_dict.items() if ev_id in selected_events}
    filtered_steps_list = [(x, y) for x, y in steps_list if x in selected_events and y in selected_events]

    return filtered_event_dict, filtered_steps_list


def data_projection_by_segment_percentage(event_dict, steps_list, coverage):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :param steps_list: a list of pairs of numbers, each number uniquely identifies an event from the log, each pair
    constitutes a step
    :param coverage: a number in (0,1] for the step (directly-follows events) coverage based on activity pair value
    frequency
    :return: for coverage e.g., 0.6 it returns a filtered event dict containing only the 60% of events covered by most
    frequent activity pairs (projects event data onto these events) and steps that correspond to those activity pairs
    """
    # Create a dictionary to count activity pairs
    activity_pair_counts = {}

    # Count activity pairs based on the list of integer pairs
    for ei, ej in steps_list:
        act_ei = event_dict[ei]['act']
        act_ej = event_dict[ej]['act']
        activity_pair = (act_ei, act_ej)
        activity_pair_counts[activity_pair] = activity_pair_counts.get(activity_pair, 0) + 1

    # Sort activity pairs by frequency in descending order
    sorted_activity_pairs = sorted(
        activity_pair_counts.items(),
        key=lambda pair: pair[1],
        reverse=True
    )

    # Calculate the number of event pairs to keep
    num_pairs_to_keep = int(len(steps_list) * coverage)

    # Collect the events involved in the most frequent activity pairs
    selected_events = set()
    for activity_pair, _ in sorted_activity_pairs:
        act_ei, act_ej = activity_pair
        # all steps of the next most frequent activity pair are added
        for ei, ej in steps_list:
            if event_dict[ei]['act'] == act_ei and event_dict[ej]['act'] == act_ej:
                selected_events.add(ei)
                selected_events.add(ej)
        # stop adding steps of a new activity pair as soon as enough steps are added w.r.t. the desired coverage
        if len(selected_events) >= num_pairs_to_keep * 2:
            break

    # Create a new dictionary with the selected events
    filtered_event_dict = {ev_id: event_data for ev_id, event_data in event_dict.items() if ev_id in selected_events}
    # Create a new list of integer pairs based on the filtered events
    filtered_steps_list = [(x, y) for x, y in steps_list if x in selected_events and y in selected_events]

    return filtered_event_dict, filtered_steps_list


def event_dict_projection(event_dict, steps_list, trigger_dict, release_dict, activity_cov, segment_cov):
    if isinstance(activity_cov, float) and 0 < activity_cov < 1:
        projected_event_dict, projected_steps_list = data_projection_by_activity_percentage(event_dict, steps_list,
                                                                                            activity_cov)
        selected_events = projected_event_dict.keys()
        projected_trigger_dict = {ei: list(set(triggered_ej).intersection(selected_events)) for ei, triggered_ej in
                                  trigger_dict.items() if ei in selected_events}
        projected_release_dict = {ej: list(set(released_ei).intersection(selected_events)) for ej, released_ei in
                                  trigger_dict.items() if ej in selected_events}

    elif isinstance(segment_cov, float) and 0 < segment_cov < 1:
        projected_event_dict, projected_steps_list = data_projection_by_segment_percentage(event_dict, steps_list,
                                                                                           segment_cov)
        selected_events = projected_event_dict.keys()
        projected_trigger_dict = {ei: list(set(triggered_ej).intersection(selected_events)) for ei, triggered_ej in
                                  trigger_dict.items() if ei in selected_events}
        projected_release_dict = {ej: list(set(released_ei).intersection(selected_events)) for ej, released_ei in
                                  trigger_dict.items() if ej in selected_events}

    else:  # the default case: both are 'all', no projection taking place
        projected_event_dict = event_dict
        projected_steps_list = steps_list
        projected_trigger_dict = trigger_dict
        projected_release_dict = release_dict
    return projected_event_dict, projected_steps_list, projected_trigger_dict, projected_release_dict

