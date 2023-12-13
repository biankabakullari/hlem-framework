import numpy as np
from collections import defaultdict
import logging
from typing import Literal, Union, NamedTuple
from component import Component
from eval_fw import Aspect


TrafficType = Literal['low', 'normal', 'high']
TrafficOfInterest = Literal['low', 'high', 'low and high']


class HLE(NamedTuple):
    aspect: Aspect
    entity: str
    traffic_type: TrafficType
    component: Component
    value: Union[float, int]
    window: int


class HLA(NamedTuple):
    aspect: Aspect
    entity: str
    traffic_type: TrafficType


def get_high_level_activity(hle: HLE):
    hla = HLA(hle.aspect,  hle.entity, hle.traffic_type)
    return hla


def get_low_and_high_thresholds(multiset, p):
    """
    :param multiset: a list of values, may contain duplicates
    :param p: must be number 50 < p < 100
    :return: a pair of values:
        -   low thresh: the (100-p)th percentile of the value list, after excluding zeros
        -   high thresh: the pth percentile of the value list, after excluding zeros
    We assume zeros represent inactive times in the process and considering them as "low load" shifts the threshold in
    an undesired way
    """
    if not 50 <= p < 100:
        raise ValueError('The percentile p for determining the threshold must be 50 <= p < 100')

    multiset_no_zeros = list(filter(lambda m: m != 0, multiset))
    if len(multiset_no_zeros) == 0:
        # if all values are 0, the high threshold is so high that no zero value qualifies as higher
        high_thresh = 10 ** 10
        # if all values are 0, the low threshold is zero so that no zero value qualifies as lower
        low_thresh = 0
    else:
        high_thresh = np.percentile(multiset_no_zeros, p)
        low_thresh = np.percentile(multiset_no_zeros, 100 - p)

    return low_thresh, high_thresh


def value_class(value, low, high) -> TrafficType:
    """
    :param value: a number
    :param low: a number, the threshold for value to be considered as low
    :param high: a number, the threshold for value to be considered as high
    :return: 'low' if value lower than low, 'high' if value higher than high, 'normal' otherwise
    """
    if value < low:
        return 'low'
    elif value > high:
        return "high"
    else:
        return "normal"


def hlf_to_value_multiset(eval_complete):
    """
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :return:
    A dict where each key is a high-level feature (e.g. exec-a, busy-Jane, cross-ab) and the value is the list of all
    measurements of that hlf across the different windows
    """
    hlf_to_value = {}

    for window in eval_complete.keys():
        eval_window = eval_complete[window]
        for hlf in eval_window.keys():
            val = eval_window[hlf]
            if hlf in hlf_to_value.keys():
                hlf_to_value[hlf].append(val)
            else:
                hlf_to_value[hlf] = [val]

    return hlf_to_value


def hlf_to_thresholds(eval_complete, p):
    """
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :param p: a number such that 50  <= p < 100
    :return:
    A dict where each key is a high-level feature (e.g. exec-a, busy-Jane, cross-ab) and the value is a pair
    (low, thresh) with the corresponding low and high thresholds w.r.t. p
    """
    hlf_to_thresh = {}
    hlf_to_value = hlf_to_value_multiset(eval_complete)

    for hlf in hlf_to_value.keys():
        value_multiset = hlf_to_value[hlf]
        low, high = get_low_and_high_thresholds(value_multiset, p)
        hlf_to_thresh[hlf] = (low, high)

    return hlf_to_thresh


def aspect_to_value_multiset(eval_complete):
    """
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :return:
     A dict where each key is a high-level feature aspect (e.g. exec, busy, cross) and the value is the list  of all
     measured values across the different entities (e.g. for key 'exec': all 'exec-a' values for all a from the activity
     set)
    """
    aspect_to_value = {}  # e.g. aspect_to_value[('activity', 'exec')] = [...]

    for window in eval_complete.keys():
        eval_window = eval_complete[window]
        for hlf in eval_window.keys():
            aspect = hlf[0]
            val = eval_window[hlf]
            if aspect in aspect_to_value.keys():
                aspect_to_value[aspect].append(val)
            else:
                aspect_to_value[aspect] = [val]

    return aspect_to_value


def get_aspect_thresholds(eval_complete, p):
    """
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :param p: a number such that 50  <= p < 100
    :return:
    A dict where each key is a high-level feature aspect (e.g. exec, busy, cross) and the value is a pair
    (low, thresh) with the corresponding low and high thresholds w.r.t. p
    """
    aspect_to_thresh = {}
    aspect_to_values = aspect_to_value_multiset(eval_complete)

    for aspect in aspect_to_values.keys():
        value_multiset = aspect_to_values[aspect]
        low, high = get_low_and_high_thresholds(value_multiset, p)
        aspect_to_thresh[aspect] = (low, high)

    return aspect_to_thresh


def hle_window_by_hlf(traffic_of_interest: TrafficOfInterest, window, eval_complete, comp_type_dict, hlf_to_thresh,
                      frequencies_last, id_counter):
    """
    :param traffic_of_interest: the type of traffic that should generate high-level-events, can be 'low', 'high', or
    'low and high'
    :param window: some window id
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :param comp_type_dict: a dictionary {'a': 'activity', ..., 'r': 'resource, ..., 's': 'segment'} for any activity a,
    resource r, and segment s
    :param hlf_to_thresh: A dict where each key is a high-level feature (e.g. exec-a, busy-Jane, cross-ab) and the value
    is a pair (low, thresh) with the corresponding low and high thresholds
    :param frequencies_last: A dict where each key is a high-level activity HLA (with aspect, entity and traffic type)
    and the value is its occurrence count across all the previous windows
    :param id_counter: a high-level event id that gets incremented for each new hle within and across windows
    :return:
    -   id_to_hle: a dict where each key is a unique id and the value is the high-level event of type HLE
    -   frequencies_last: A dict where each key is a high-level activity HLA (with aspect, entity and traffic type) and
    the value is its occurrence count after the update in the current window
    -   id_counter: the id of the last detected high-level event
    """

    id_to_hle = {}
    eval_at_window = eval_complete[window]

    for hlf in eval_at_window.keys():
        hlf_value = eval_at_window[hlf]
        aspect = hlf[0]  # e.g. busy
        entity = hlf[1]  # e.g. Jane
        component = comp_type_dict[entity]
        low, high = hlf_to_thresh[hlf]
        traffic = value_class(hlf_value, low, high)

        if traffic in traffic_of_interest:
            hle = HLE(aspect, entity, traffic, component, hlf_value, window)
            # the id_counter is the key of the generated hle within the window, window[id_counter] = hle
            # it gets updated after each window so that even across windows, the ids are unique
            id_to_hle[id_counter] = hle
            hla = HLA(aspect, entity, traffic)
            frequencies_last[hla] += 1
            id_counter += 1

    return id_to_hle, frequencies_last, id_counter


def hle_window_by_aspect(traffic_of_interest: TrafficOfInterest, window, eval_complete, comp_type_dict,
                         aspect_to_thresholds, frequencies_last, id_counter):
    """
    :param traffic_of_interest: the type of traffic that should generate high-level-events, can be 'low', 'high', or
    'low and high'
    :param window: some window id
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :param comp_type_dict: a dictionary {'a': 'activity', ..., 'r': 'resource, ..., 's': 'segment'} for any activity a,
    resource r, and segment s
    :param aspect_to_thresholds: A dict where each key is a high-level aspect (e.g. exec, busy, cross) and the value
    is a pair (low, thresh) with the corresponding low and high thresholds
    :param frequencies_last: A dict where each key is a high-level activity HLA (with aspect, entity and traffic type)
    and the value is its occurrence count across all the previous windows
    :param id_counter: the id of the last detected high-level event
    :return:
    -   id_to_hle: a dict where each key is a unique id and the value is the high-level event of type HLE
    -   frequencies_last: A dict where each key is a high-level activity HLA (with aspect, entity and traffic type) and
    the value is its occurrence count after the update in the current window
    -   id_counter: the id of the last detected high-level event
    """

    id_to_hle = {}
    eval_at_window = eval_complete[window]

    for hlf in eval_at_window.keys():
        hlf_value = eval_at_window[hlf]
        aspect = hlf[0]  # e.g. busy
        entity = hlf[1]  # e.g. Jane
        component = comp_type_dict[entity]
        low, high = aspect_to_thresholds[aspect]
        traffic = value_class(hlf_value, low, high)

        if traffic in traffic_of_interest:
            hle = HLE(aspect, entity, traffic, component, hlf_value, window)
            # the id_counter is the key of the generated hle within the window, window[id_counter] = hle
            # it gets updated after each window so that even across windows, the ids are unique
            id_to_hle[id_counter] = hle
            hla = HLA(aspect, entity, traffic)
            frequencies_last[hla] += 1
            id_counter += 1
    return id_to_hle, frequencies_last, id_counter


def generate_hle(traffic_of_interest, eval_complete, comp_type_dict, p, aspect_based):
    """
    :param traffic_of_interest: the type of traffic that should generate high-level-events, can be 'low', 'high', or
    'low and high'
    :param eval_complete: a dict with first level key value pairs: window id, dictionary for that window, and second
    level key value pairs: {(enqueue,a):v1, (enqueue,b):v2,...}, {(enter,(a,b)):w1, (enter,(c,d)):w2,...},
    {(busy,r1):y1, (busy,r2):y2...} with the corresponding counts for the window + high-level feature combination
    :param comp_type_dict: a dictionary {'a': 'activity', ..., 'r': 'resource, ..., 's': 'segment'} for any activity a,
    resource r, and segment s
    :param p: must be number 50 < p < 100
    :param aspect_based: If True, the thresholds are determined based on aspect (e.g., exec, enter, busy). If False, the
    thresholds are determined based on high-level feature (e.g. 'exec-a', 'busy-Jane').
    :return:
    -   id_to_hle_all: a dict where each key is a unique id and the value is the high-level event of type HLE
    -   window_to_id_to_hle: a dict where first key is the window id, second key is the high-level event id and the value
    is the HLE (aspect, entity, component, traffic_type, value, window)
    -   last_freq: A dict where each key is a high-level activity HLA (with aspect, entity and traffic type) and
    the value is its occurrence count across all windows
    """
    id_to_hle_all = {}
    window_to_id_to_hle = {}

    hla_frequencies = defaultdict(lambda: 0)
    last_freq = hla_frequencies
    id_counter = 0

    # the threshold to generate a hle of hlf=('busy','Jane') will depend on all values computed for 'busy'
    if aspect_based:
        aspect_to_thresh = get_aspect_thresholds(eval_complete, p)
        for window in eval_complete.keys():
            id_to_hle_window, last_freq_updated, id_counter = hle_window_by_aspect(traffic_of_interest, window,
                                                                                   eval_complete, comp_type_dict,
                                                                                   aspect_to_thresh, last_freq,
                                                                                   id_counter)
            last_freq = last_freq_updated
            window_to_id_to_hle[window] = id_to_hle_window
            for hle_id in id_to_hle_window.keys():
                hle = id_to_hle_window[hle_id]
                id_to_hle_all[hle_id] = hle

    # the threshold to generate a hle of hlf=('busy','Jane') will depend (only) on all values computed for \
    # ('busy','Jane')
    else:
        hlf_to_thresh = hlf_to_thresholds(eval_complete, p)
        for window in eval_complete.keys():
            id_to_hle_window, last_freq_updated, id_counter = hle_window_by_hlf(traffic_of_interest, window,
                                                                                eval_complete, comp_type_dict,
                                                                                hlf_to_thresh, last_freq,
                                                                                id_counter)

            last_freq = last_freq_updated
            window_to_id_to_hle[window] = id_to_hle_window
            for hle_id in id_to_hle_window.keys():
                hle = id_to_hle_window[hle_id]
                id_to_hle_all[hle_id] = hle
    no_hle = len(id_to_hle_all.keys())
    logging.info('We detected ' + str(no_hle) + ' high-level events.')

    return id_to_hle_all, window_to_id_to_hle, last_freq


def filter_hla(freq_dict, freq_thresh):
    """
    :param freq_dict: A dict where each key is a high-level activity HLA (with aspect, entity and traffic type) and
    the value is its occurrence count across all windows
    :param freq_thresh:
    :return:
    """
    hla_filtered = []
    freq_values = [freq_dict[hla] for hla in freq_dict.keys()]

    # a freq_thresh=0.8 requests selecting only the 20% most frequent high-level activities
    if 0 < freq_thresh < 1:
        percentile = freq_thresh * 100
        _, high = get_low_and_high_thresholds(freq_values, percentile)

        for hla in freq_dict.keys():
            if freq_dict[hla] >= high:
                hla_filtered.append(hla)
        return hla_filtered

    # a freq_thresh=7 requests selecting the seven most frequent high-level activities
    elif freq_thresh >= 1 and isinstance(freq_thresh, int):
        most_frequent = sorted(freq_dict.keys(), key=lambda x: freq_dict[x])[:freq_thresh]
        return most_frequent

    # freq_thresh = 0 means no filtering required
    elif freq_thresh == 0:
        hla_unfiltered = freq_dict.keys()
        return hla_unfiltered

    else:
        raise ValueError('The freq_thresh to filter high-level activities must be a float (0,1) or an integer >=1')
