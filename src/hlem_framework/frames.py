import math
from datetime import datetime, timezone


def days_since_epoch(datetime_ts):
    """
    mapping timestamps onto numbers, here: #days elapsed from the Unix time, events happening
    on the same day will get the same number
    """
    int_days = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).days
    return int_days


def hours_since_epoch(datetime_ts):
    """
    mapping timestamps onto numbers, here: #hours elapsed from the Unix time, events happening
    within the same hour will get the same number
    """
    int_seconds = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    int_hours = math.ceil(int_seconds/3600)
    return int_hours


def minutes_since_epoch(datetime_ts):
    int_seconds = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    int_minutes = math.ceil(int_seconds/60)
    return int_minutes


def time_unit_of_timestamp(datetime_ts, unit):

    if unit == 'days':
        time_unit = days_since_epoch(datetime_ts)
    elif unit == 'hours':
        time_unit = hours_since_epoch(datetime_ts)
    else:  # unit == 'minutes':
        time_unit = minutes_since_epoch(datetime_ts)
    return time_unit


def seconds_since_epoch(datetime_ts):
    """
    mapping timestamps onto numbers, here: #seconds elapsed from the Unix time
    """
    if isinstance(datetime_ts, str):
        datetime_ts = datetime.strptime(datetime_ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    int_seconds = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    try:
        int_seconds < 0
    except ValueError:
        print("There are events in the data that have happened prior to January 1, 1970.")
    return int_seconds


def seconds_to_datetime(int_number, tz_info):
    timestamp = datetime.fromtimestamp(int_number, tz_info)
    return timestamp


def sorted_ids_by_ts(event_dict):
    """

    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :return: a list of the keys (event IDs) sorted by timestamp
    """
    ids_sorted = sorted(event_dict.keys(), key=lambda x: event_dict[x]['ts-seconds'])
    return ids_sorted


def get_window_size_from_number(event_dict, number):
    """
    Given the events and desired number of windows, the size of each window is determined (all windows equally spread
    between the first and last recorded timestamp)
    """
    ids_sorted = sorted_ids_by_ts(event_dict)
    id_min = ids_sorted[0]
    id_max = ids_sorted[-1]

    start_int = event_dict[id_min]['ts-seconds']
    end_int = event_dict[id_max]['ts-seconds']

    window_size = math.ceil((end_int - start_int)/number)

    return window_size


def window_borders_dict_by_size(event_dict, window_size, ids_sorted):
    """

    Given the events and desired window size (width), returns dictionary where each key is a number identifying a window
    and each value=(left_border, right_border) a tuple containing the borders of the window
    """
    id_min = ids_sorted[0]
    id_max = ids_sorted[-1]

    start_int = event_dict[id_min]['ts-seconds']
    end_int = event_dict[id_max]['ts-seconds']

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


def window_events_dict_by_size(event_dict, window_size):

    """
    :param event_dict: events
    :param window_size: desired window size
    :return:
    -   w_events_list: dict with window identifiers as keys and list of event IDs that occur within that window as
    values
    -   id_window_mapping: dict where id_window_mapping[e_id]=w whenever an event e with id e_id occurs within window w
    """

    ids_sorted = sorted_ids_by_ts(event_dict)
    w_borders_dict = window_borders_dict_by_size(event_dict, window_size, ids_sorted)

    # initially, each window is empty
    # some windows might remain empty, but we still want them to exist
    w_events_list = {w: [] for w in list(w_borders_dict.keys())}

    #assign corresponding frame to each event
    id_window_mapping = {ev: 0 for ev in list(event_dict.keys())}

    current_window = 0
    max_window = max([windowId for windowId in w_events_list.keys()])
    current_borders = w_borders_dict[current_window]

    for ev_id in ids_sorted:
        current_ts = seconds_since_epoch(event_dict[ev_id]['ts'])
        # as long as current timestamp is higher than right border, go to next window
        while current_ts >= current_borders[1] and current_window < max_window:
            current_window += 1
            current_borders = w_borders_dict[current_window]
        w_events_list[current_window].append(ev_id)
        id_window_mapping[ev_id] = current_window

    return w_events_list, id_window_mapping


def window_events_dict_by_time_unit(event_dict, unit):

    """
    :param event_dict: events
    :param unit: day/hour
    :return:
    -   w_events_list: dict with window identifiers as keys and list of event IDs that occur within that window as
    values
    -   id_window_mapping: dict where id_window_mapping[e_id]=w whenever an event e with id e_id occurs within window w
    """

    ids_sorted = sorted_ids_by_ts(event_dict)

    id_min = ids_sorted[0]
    id_max = ids_sorted[-1]

    first_unit = time_unit_of_timestamp(event_dict[id_min]['ts'], unit)
    last_unit = time_unit_of_timestamp(event_dict[id_max]['ts'], unit)

    # there is a window for each unit
    windows = [i - first_unit for i in range(first_unit, last_unit+1)]

    # initially, each window is empty
    # some windows might remain empty, but we still want them to exist
    w_events_list = {w: [] for w in windows}

    #assign corresponding frame to each event, initially all mapped to first window
    id_window_mapping = {ev: 0 for ev in list(event_dict.keys())}

    # the border of each window corresponds to the start and end of the time unit (in seconds!!!)
    # w_borders_dict = {}

    for ev_id in ids_sorted:
        current_unit = time_unit_of_timestamp(event_dict[ev_id]['ts'], unit)
        current_window = current_unit - first_unit
        w_events_list[current_window].append(ev_id)
        id_window_mapping[ev_id] = current_window

    return w_events_list, id_window_mapping


def framing(event_dict, frame):

    if type(frame) == int or type(frame) == float:
        number = frame
        window_size = get_window_size_from_number(event_dict, number)
        w_events_list, id_window_mapping = window_events_dict_by_size(event_dict, window_size)
    else:
        unit = frame
        w_events_list, id_window_mapping = window_events_dict_by_time_unit(event_dict, unit)

    return w_events_list, id_window_mapping


def window_borders_dict_by_time_unit(event_dict, unit, ids_sorted):
    """

    Given the events and desired window size (width), returns dictionary where each key is a number identifying a window
    and each value=(left_border, right_border) a tuple containing the borders of the window
    """
    w_borders_dict = {}

    id_min = ids_sorted[0]
    id_max = ids_sorted[-1]

    first_unit = time_unit_of_timestamp(event_dict[id_min]['ts'], unit)
    last_unit = time_unit_of_timestamp(event_dict[id_max]['ts'], unit)

    # there is a window for each unit
    windows = [i - first_unit for i in range(first_unit, last_unit+1)]

    for w in windows:
        current_unit = w + first_unit
        if unit == 'hours':
            left_border = (current_unit-1) * 3600
            right_border = current_unit * 3600
        elif unit == 'minutes':
            left_border = (current_unit-1) * 60
            right_border = current_unit * 60
        else:  # unit = 'days'
            left_border = (current_unit-1) * 24*3600
            right_border = current_unit * 24*3600

        w_borders_dict[w] = (left_border, right_border)

    return w_borders_dict


def windows_borders_dict(event_dict, frame, ids_sorted):
    if type(frame) == int or type(frame) == float:
        number = frame
        size = get_window_size_from_number(event_dict, number)
        windows_borders = window_borders_dict_by_size(event_dict, size, ids_sorted)
    else:
        unit = frame
        windows_borders = window_borders_dict_by_time_unit(event_dict, unit, ids_sorted)
    return windows_borders
