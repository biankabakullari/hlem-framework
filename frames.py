import math


# given event_dict, returns a list of the keys (event IDs) sorted by timestamp
def sorted_ids_by_timestamp(event_dict):
    ids_sorted = sorted(event_dict.keys(), key=lambda x: event_dict[x]['ts'])
    return ids_sorted


# obtain smallest timestamp in the log
def min_ts(event_dict, ids_sorted):
    id_min = ids_sorted[0]
    ts_min = event_dict[id_min]['ts']
    return ts_min


# obtain highest timestamp in the log
def max_ts(event_dict, ids_sorted):
    id_max = ids_sorted[-1]
    ts_max = event_dict[id_max]['ts']
    return ts_max


# given start and end ts of the log data and width of window, obtain dictionary with
# keys=bucket number, value=(left_border, right_border) as numbers
def bucket_window_dict_by_width(event_dict, width):

    ids_sorted = sorted_ids_by_timestamp(event_dict)
    start_int = min_ts(event_dict, ids_sorted)
    end_int = max_ts(event_dict, ids_sorted)

    bucket_window_dict = {}
    b = 0

    current_left = start_int
    current_right = current_left + width
    while current_right < end_int:
        bucket_window_dict[b] = (current_left, current_right)
        current_left = current_right
        current_right = current_left + width
        b += 1

    return bucket_window_dict


def get_width_from_number(event_dict, number):

    ids_sorted = sorted_ids_by_timestamp(event_dict)

    start_int = min_ts(event_dict, ids_sorted)
    end_int = max_ts(event_dict, ids_sorted)

    width = math.ceil((end_int - start_int)/number)

    return width


# given event dictionary and window size
# create dict with bucket numbers as keys and list of event IDs that happen within corresponding window as values
def bucket_id_list_dict_by_width(event_dict, width):

    ids_sorted = sorted_ids_by_timestamp(event_dict)
    bucket_window_dict = bucket_window_dict_by_width(event_dict, width)

    # initially, each window bucket is empty
    # some window buckets might remain empty
    bucket_id_list = {b: [] for b in bucket_window_dict.keys()}

    #assign corresponding frame to each event
    id_bucket_mapping = {ev_id: 0 for ev_id in event_dict.keys()}

    curr_bucket = 0
    max_bucket = max([bucketId for bucketId in bucket_id_list.keys()])
    curr_window = bucket_window_dict[curr_bucket]

    for ev_id in ids_sorted:
        curr_ts = event_dict[ev_id]['ts']

        # as long as current timestamp is higher than right border, got to next window
        while curr_ts >= curr_window[1] and curr_bucket < max_bucket:
            curr_bucket += 1

            curr_window = bucket_window_dict[curr_bucket]
        bucket_id_list[curr_bucket].append(ev_id)
        id_bucket_mapping[ev_id] = curr_bucket

    return bucket_id_list, id_bucket_mapping


def increase_window_number(p, no_windows):
    new_number = no_windows + p*no_windows
    return new_number


def decrease_window_number(p, no_windows):
    new_number = no_windows - p*no_windows
    return new_number
