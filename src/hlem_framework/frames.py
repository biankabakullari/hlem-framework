import math
from datetime import datetime, timezone


def weeks_since_epoch(datetime_ts):
    """
    :param datetime_ts: a datetime timestamp
    :return: number of weeks elapsed since the Unix time, events happening on the same week will get the same number
    """
    int_days = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).days
    int_weeks = math.ceil(int_days/7)
    return int_weeks


def days_since_epoch(datetime_ts):
    """
    :param datetime_ts: a datetime timestamp
    :return: number of days elapsed since the Unix time, events happening on the same day will get the same number
    """
    int_days = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).days
    return int_days


def hours_since_epoch(datetime_ts):
    """
    :param datetime_ts: a datetime timestamp
    :return: number of hours elapsed since the Unix time, events happening within the same hour will get the same number
    """
    int_seconds = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    int_hours = math.ceil(int_seconds/3600)
    return int_hours


def minutes_since_epoch(datetime_ts):
    """
    :param datetime_ts: a datetime timestamp
    :return: number of minutes elapsed since the Unix time, events happening within the same hour will get the same number
    """
    int_seconds = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    int_minutes = math.ceil(int_seconds/60)
    return int_minutes


def seconds_since_epoch(datetime_ts):
    """
    :param datetime_ts: a datetime timestamp
    :return: number of seconds elapsed since the Unix time
    """
    if isinstance(datetime_ts, str):
        datetime_ts = datetime.strptime(datetime_ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    int_seconds = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    return int_seconds


def time_unit_of_timestamp(datetime_ts, unit):
    """
    :param datetime_ts: a datetime timestamp
    :param unit: can be 'minutes', 'hours', or 'days'
    :return: number of units elapsed since the Unix time
    """
    try:
        unit not in ['seconds', 'minutes', 'hours', 'days', 'weeks']
    except ValueError:
        print("The time unit must be minutes/hours/days/weeks")
    if unit == 'days':
        time_unit = days_since_epoch(datetime_ts)
    elif unit == 'hours':
        time_unit = hours_since_epoch(datetime_ts)
    elif unit == 'minutes':
        time_unit = minutes_since_epoch(datetime_ts)
    else: # unit = weeks
        try: unit == 'weeks'
        except ValueError:
            'The time unit can only be set to minutes, hours, days, or weeks.'
        time_unit = weeks_since_epoch(datetime_ts)

    return time_unit


def seconds_to_datetime(int_seconds_since_epoch, tz_info):
    """
    :param int_seconds_since_epoch: seconds since epoch
    :param tz_info: timezone info
    :return: timestamp as a datetime object
    """
    timestamp = datetime.fromtimestamp(int_seconds_since_epoch, tz_info)
    return timestamp


def sorted_ids_by_ts(event_dict):
    """
    :param event_dict: dictionary where keys are numbers identifying events, values are the event attribute-value pairs
    :return: a list of the keys (event IDs) sorted by timestamp
    """
    ids_sorted = sorted(event_dict.keys(), key=lambda x: event_dict[x]['ts-seconds'])
    return ids_sorted


def get_window_size_from_unit(unit):
    """
    :param unit: 'minutes', 'days', 'hours', or 'weeks'
    :return: The time unit turned into seconds
    """
    if unit == 'minutes':
        size = 60
    elif unit == 'hours':
        size = 3600
    elif unit == 'days':
        size = 24 * 3600
    else:  # unit = weeks
        try:
            unit == 'weeks'
        except ValueError:
            'The time unit can only be set to minutes, hours, days, or weeks.'
        size = 7 * 24 * 3600

    return size


def get_window_size_from_number(event_dict, number, ids_sorted):
    """

    :param event_dict: an event dictionary {0: {'act': a, 'case': 5, 'res':'r', ...}, ...}
    :param number: desired number of time windows to split the events into
    :return:
    The window size  in seconds as integer (all windows have equal size between the first and last recorded timestamp)
    """

    id_min = ids_sorted[0]
    id_max = ids_sorted[-1]

    start_int = event_dict[id_min]['ts-seconds']
    end_int = event_dict[id_max]['ts-seconds']

    window_size = math.ceil((end_int - start_int)/number)

    return window_size


def window_borders_dict(event_dict, window_size, ids_sorted):

    """
    :param event_dict: an event dictionary {0: {'act': a, 'case': 5, 'res':'r', ...}, ...}
    :param window_size: desired window size as number of seconds (integer)
    :param ids_sorted: the event ids sorted by event timestamp
    :return: dictionary where each key is a number identifying a window and each value=(left_border, right_border) is
    a tuple containing the borders of the window in seconds
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


def window_events_dict(event_dict, window_size, ids_sorted):

    """
    :param event_dict: an event dictionary {0: {'act': a, 'case': 5, 'res':'r', ...}, ...}
    :param window_size: desired window size
    :return:
    -   w_events_list: dict with window identifiers as keys and list of event IDs that occur within that window
    (in [left_border, right_border)) as values
    -   id_window_mapping: dict where id_window_mapping[e_id]=w whenever an event e with id e_id occurs within window w
    (in [left_border, right_border))
    """
    w_borders_dict = window_borders_dict(event_dict, window_size, ids_sorted)

    # initially, each window is empty
    # some windows might remain empty, but we still want them to exist
    w_events_list = {w: [] for w in list(w_borders_dict.keys())}

    #assign corresponding frame to each event
    id_window_mapping = {ev: 0 for ev in list(event_dict.keys())}

    current_window = 0
    max_window = max([windowId for windowId in w_events_list.keys()])
    current_borders = w_borders_dict[current_window]

    for ev_id in ids_sorted:
        current_ts = event_dict[ev_id]['ts-seconds']
        # as long as current timestamp is higher than right border of current window, go to next window
        while current_ts >= current_borders[1] and current_window < max_window:
            current_window += 1
            current_borders = w_borders_dict[current_window]
        w_events_list[current_window].append(ev_id)
        id_window_mapping[ev_id] = current_window

    return w_events_list, id_window_mapping


def frame_events_dict(event_dict, frame, ids_sorted):

    """
    :param event_dict: an event dictionary {0: {'act': a, 'case': 5, 'res':'r', ...}, ...}
    :param frame: can be a number (determining number of windows) or a time unit (minutes, hours, days, or weeks)
    :return:
    -   w_events_list: dict with window identifiers as keys and list of event IDs that occur within that window
    (in [left_border, right_border)) as values
    -   id_window_mapping: dict where id_window_mapping[e_id]=w whenever an event e with id e_id occurs within window w
    (in [left_border, right_border))
    """

    if type(frame) == int or type(frame) == float:
        number = frame
        window_size = get_window_size_from_number(event_dict, number, ids_sorted)

    else:
        unit = frame
        assert unit in ['minutes', 'hours', 'days', 'weeks']
        window_size = get_window_size_from_unit(unit)
    w_events_list, id_window_mapping = window_events_dict(event_dict, window_size, ids_sorted)

    return w_events_list, id_window_mapping


def frame_borders_dict(event_dict, frame, ids_sorted):

    """
    :param event_dict: event_dict: an event dictionary {0: {'act': a, 'case': 5, 'res':'r', ...}, ...}
    :param frame: frame: can be a number (determining number of windows) or a time unit (minutes, hours, days, or weeks)
    :param ids_sorted: the event ids sorted by event timestamp
    :return: dictionary where each key is a number identifying a window and each value=(left_border, right_border) is
    a tuple containing the borders of the window in seconds
    """

    if type(frame) == int or type(frame) == float:
        number = frame
        size = get_window_size_from_number(event_dict, number, ids_sorted)
    else:
        unit = frame
        size = get_window_size_from_unit(unit)
    windows_borders = window_borders_dict(event_dict, size, ids_sorted)
    return windows_borders
