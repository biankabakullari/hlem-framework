import math


def sorted_ids_by_ts(event_dict):
    """

    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :return: a list of the keys (event IDs) sorted by timestamp
    """
    ids_sorted = sorted(event_dict.keys(), key=lambda x: event_dict[x]['ts'])
    return ids_sorted


def min_ts(event_dict, ids_sorted):
    """
    obtain the smallest timestamp in the log (timestamp is an integer = seconds elapsed since Unix time)
    """
    id_min = ids_sorted[0]
    ts_min = event_dict[id_min]['ts']
    return ts_min


def max_ts(event_dict, ids_sorted):
    """
    obtain the largest timestamp in the log (timestamp is an integer = seconds elapsed since Unix time)
    """
    id_max = ids_sorted[-1]
    ts_max = event_dict[id_max]['ts']
    return ts_max


def get_window_size(event_dict, number):
    """
    Given the events and desired number of windows, the size of each window is determined (all windows equally spread
    between the first and last recorded timestamp)
    """
    ids_sorted = sorted_ids_by_ts(event_dict)

    start_int = min_ts(event_dict, ids_sorted)
    end_int = max_ts(event_dict, ids_sorted)

    window_size = math.ceil((end_int - start_int)/number)

    return window_size


def window_borders_dict(event_dict, window_size):
    """

    Given the events and desired window size (width), returns dictionary where each key is a number identifying a window
    and each value=(left_border, right_border) a tuple containing the borders of the window
    """
    ids_sorted = sorted_ids_by_ts(event_dict)
    start_int = min_ts(event_dict, ids_sorted)
    end_int = max_ts(event_dict, ids_sorted)

    w_borders_dict = {}
    w = 0

    current_left = start_int
    current_right = current_left + window_size
    while current_right < end_int:
        w_borders_dict[w] = (current_left, current_right)
        current_left = current_right
        current_right = current_left + window_size
        w += 1

    return w_borders_dict


def window_events_dict(event_dict, window_size):

    """
    :param event_dict: events
    :param window_size: desired window size
    :return:
    -   w_events_list: dict with window identifiers as keys and list of event IDs that occur within that window as
    values
    -   id_window_mapping: dict where id_window_mapping[e_id]=w whenever an event e with id e_id occurs within window w
    """

    ids_sorted = sorted_ids_by_ts(event_dict)
    w_borders_dict = window_borders_dict(event_dict, window_size)

    # initially, each window is empty
    # some windows might remain empty, but we still want them to exist
    w_events_list = dict.fromkeys(list(w_borders_dict.keys()), [])

    #assign corresponding frame to each event
    id_window_mapping = dict.fromkeys(list(event_dict.keys()), 0)

    current_window = 0
    max_window = max([bucketId for bucketId in w_events_list.keys()])
    current_borders = w_borders_dict[current_window]

    for ev_id in ids_sorted:
        current_ts = event_dict[ev_id]['ts']

        # as long as current timestamp is higher than right border, go to next window
        while current_ts >= current_borders[1] and current_window < max_window:
            current_window += 1
            current_borders = w_borders_dict[current_window]
        w_events_list[current_window].append(ev_id)
        id_window_mapping[ev_id] = current_window

    return w_events_list, id_window_mapping


# def increase_window_number(p, no_windows):
#     new_number = no_windows + p*no_windows
#     return new_number
#
#
# def decrease_window_number(p, no_windows):
#     new_number = no_windows - p*no_windows
#     return new_number
