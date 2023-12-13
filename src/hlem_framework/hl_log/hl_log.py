from dataclasses import dataclass

import pandas as pd
import os
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import frames
from hle_generation.hle_generation_fw import HLA, TrafficOfInterest, get_high_level_activity


@dataclass
class HlLogArgs:
    """
    only_entity: if True, high-level activities like 'exec-a' will only show activity label 'a'
    traffic_of_interest: if only 'low' or 'high', it will not be shown in the activity names of the high-level
    log (as it would always be the same)
    """
    only_entity: bool
    traffic_of_interest: TrafficOfInterest


def create_hle_table(case_column, activity_column, timestamp_column, value_column, component_column):
    """
    :param case_column: a list with (high-level) case ids as values
    :param activity_column: a list with (high-level) activity labels as values
    :param timestamp_column: a list with timestamps as values
    :param value_column: a list with numbers as values
    :param component_column: a list with components ('activity', 'segment', or 'resource') as values
    :return: a dataframe containing the input columns
    """
    table = pd.DataFrame(list(zip(case_column, activity_column, timestamp_column, value_column, component_column)),
                         columns=['case', 'concept:name', 'time:timestamp', 'value', 'entity'])

    return table


def hla_to_string(hla: HLA, args: HlLogArgs):
    """
    :param hla: the high-level activity, a named tuple with attributes aspect, entity, and traffic_type
    :param args: parameters only_entity and traffic_of_interest of class HlLogArgs
    :return: the string that will be used as activity name in the high-level log, for hla 'exec-a-high', it can be
    'exec-a', 'a-high', or 'a' depending on the parameters
    """
    hla_string = []

    aspect, entity, traffic_type = hla.aspect, hla.entity, hla.traffic_type

    # if entity is a segment
    if not isinstance(entity, int) and len(entity) == 2:
        a = entity[0]
        b = entity[1]
        entity_string = '(' + str(a) + '-->' + str(b) + ')'
    else:
        entity_string = str(entity)

    if not args.only_entity:
        # rewrite aspects 'wait' and 'busy' as 'delay' and 'wl' as in the framework paper
        aspect_string = str(aspect)
        if aspect_string == 'wait':
            aspect_string = 'delay'
        elif aspect_string == 'busy':
            aspect_string = 'wl'
        hla_string.append(aspect_string)

    hla_string.append(entity_string)
    # only attach traffic type info if both low and high traffic are of interest
    if args.traffic_of_interest == 'low and high':
        traffic_string = str(traffic_type)
        hla_string.append(traffic_string)

    hla_string = '-'.join([string for string in hla_string])

    return hla_string


def get_table_data_po(window_to_border, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered, args: HlLogArgs):
    """
    :param window_to_border: dictionary where each key is a number identifying a window and each
    value=(left_border, right_border) is a tuple containing the borders of the window in seconds
    :param window_to_id_to_hle: a dict where first key is the window id, second key is the high-level event id and the
    value is the HLE (aspect, entity, component, traffic_type, value, window)
    :param cascade_dict: a dictionary assigning IDs to hle, such that two hle get the same ID iff they are directly or
    indirectly connected through the link values
    :param tz_info: UTC
    :param hla_filtered: list of high-level activities that should appear in the high-level log
    :param args: parameters only_entity and traffic_of_interest of class HlLogArgs
    :return: columns containing the high-level case ids, activity labels, timestamps, measured values and components so
    that the timestamp of each hle is its window's left border
    """
    case_column = []
    activity_column = []
    timestamp_column = []
    value_column = []
    component_column = []

    for w in window_to_border.keys():
        w_start_int = window_to_border[w][0]
        w_start_ts = frames.seconds_to_datetime(w_start_int, tz_info)
        hle_of_w = window_to_id_to_hle[w]
        for hle_id in hle_of_w.keys():
            hle = hle_of_w[hle_id]
            hla = get_high_level_activity(hle)
            if hla in hla_filtered:
                case_id = cascade_dict[(w, hle_id)]
                hla_string = hla_to_string(hla, args)
                val = hle.value
                comp = hle.component
                case_column.append(case_id)
                activity_column.append(hla_string)
                timestamp_column.append(w_start_ts)
                value_column.append(val)
                component_column.append(comp)

    return case_column, activity_column, timestamp_column, value_column, component_column


def get_table_data_flat(window_to_border, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered, args: HlLogArgs):
    """
    :param window_to_border: dictionary where each key is a number identifying a window and each
    value=(left_border, right_border) is a tuple containing the borders of the window in seconds
    :param window_to_id_to_hle: a dict where first key is the window id, second key is the high-level event id and the
    value is the HLE (aspect, entity, component, traffic_type, value, window)
    :param cascade_dict: a dictionary assigning IDs to hle, such that two hle get the same ID iff they are directly or
    indirectly connected through the link values
    :param tz_info: UTC
    :param hla_filtered: list of high-level activities that should appear in the high-level log
    :param args: parameters only_entity and traffic_of_interest of class HlLogArgs
    :return: columns containing the high-level case ids, activity labels, timestamps, measured values and components so
    that the timestamp same cascade same window hle are all different (based on their lexicographical order)
    """
    case_column = []
    activity_column = []
    timestamp_column = []
    value_column = []
    component_column = []

    time_granularity_int = window_to_border[0][1] - window_to_border[0][0]  # window width in seconds
    windows = list(window_to_border.keys())

    for w in windows:
        left_border_in_seconds = window_to_border[w][0]
        hle_of_w = window_to_id_to_hle[w]
        hle_of_w_filtered = [hle_id for hle_id in hle_of_w.keys() if get_high_level_activity(hle_of_w[hle_id]) in
                             hla_filtered]

        if len(hle_of_w_filtered) == 1:
            # only one hle in the window, no need to introduce a total order
            id_0 = hle_of_w_filtered[0]
            hle_0 = hle_of_w[id_0]
            hla_0 = get_high_level_activity(hle_0)
            case_id = cascade_dict[(w, id_0)]
            hla_string = hla_to_string(hla_0, args)
            val = hle_0.value
            comp = hle_0.component
            case_column.append(case_id)
            activity_column.append(hla_string)
            w_time_ts = frames.seconds_to_datetime(left_border_in_seconds, tz_info)
            timestamp_column.append(w_time_ts)
            value_column.append(val)
            component_column.append(comp)
        else:
            # at least two hle in the same window, total order defined by sorting hla lexicographically
            # timestamps are spread across the window borders
            hle_w_filtered = [hle_of_w[hle_id] for hle_id in hle_of_w_filtered]
            hla_of_w = [get_high_level_activity(hle) for hle in hle_w_filtered]
            hla_strings = [hla_to_string(hla, args) for hla in hla_of_w]
            hla_strings.sort()
            number = len(hla_strings)
            for i, hle_id in enumerate(hle_of_w_filtered):
                case_id = cascade_dict[(w, hle_id)]
                case_column.append(case_id)
                activity_column.append(hla_strings[i])
                # if there are 5 events, they will get timestamps start + 0, ,...., start + 4/5*window_width
                w_hla_int = left_border_in_seconds + ((i/number)*time_granularity_int)
                w_time_ts = frames.seconds_to_datetime(w_hla_int, tz_info)
                timestamp_column.append(w_time_ts)
                hle = hle_of_w[hle_id]
                val = hle.value
                comp = hle.component
                value_column.append(val)
                component_column.append(comp)

    return case_column, activity_column, timestamp_column, value_column, component_column


def create_dataframe(window_to_border, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered, args: HlLogArgs,
                     flatten):
    """
    :param window_to_border: dictionary where each key is a number identifying a window and each
    value=(left_border, right_border) is a tuple containing the borders of the window in seconds
    :param window_to_id_to_hle: a dict where first key is the window id, second key is the high-level event id and the
    value is the HLE (aspect, entity, component, traffic_type, value, window)
    :param cascade_dict: a dictionary assigning IDs to hle, such that two hle get the same ID iff they are directly or
    indirectly connected through the link values
    :param tz_info: UTC
    :param hla_filtered: list of high-level activities that should appear in the high-level log
    :param args: parameters only_entity and traffic_of_interest of class HlLogArgs
    :param flatten: if True, hle within the same window of the same cascade get slightly different ts. Otherwise, hle of
    same window of same cascade will have identical ts (= the window's left border)
    :return: columns containing the high-level case ids, activity labels, timestamps, measured values and components
    """
    if flatten:  # flatten the data, so that hle within the same window of the same cascade get slightly different ts
        case_column, act_column, ts_column, val_column, comp_type_column = get_table_data_flat(window_to_border,
                                                                                               window_to_id_to_hle,
                                                                                               cascade_dict, tz_info,
                                                                                               hla_filtered,
                                                                                               args)

    else:  # no flattening, hle of same window of same cascade will have identical ts = window's left border
        case_column, act_column, ts_column, val_column, comp_type_column = get_table_data_po(window_to_border,
                                                                                             window_to_id_to_hle,
                                                                                             cascade_dict, tz_info,
                                                                                             hla_filtered,
                                                                                             args)

    table = create_hle_table(case_column, act_column, ts_column, val_column, comp_type_column)

    return table


def df_to_xes_log(df):
    """
    :param df: dataframe of high-level log
    :return: high-level log in xes format
    """
    parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case'}
    hl_event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

    return hl_event_log


def export_hl_log(log, path):
    """
    :param log: input log
    :param path: the path in the dir where the log must be saved
    :return:
    """
    xes_exporter.apply(log, path)


def get_max_counter(file_type):
    """
    :param file_type: file ending, can be '.csv' or '.xes'
    :return: the counter of the last file with that ending that was exported in current dir
    """
    # Get a list of all files in the current directory
    files = os.listdir()

    # Filter files that match the expected naming pattern
    matching_files = [file for file in files if file.startswith("high_level_log") and file.endswith(file_type)]

    # Extract the counters from the file names and get the maximum value
    counters = [int(file.split("_")[2].split(".")[0]) for file in matching_files]
    max_counter = max(counters, default=0)

    return max_counter


def generate_hl_xes_and_df(window_border_dict, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered,
                                      args: HlLogArgs, flatten: bool, export: bool):
    """
    :param window_border_dict: dictionary where each key is a number identifying a window and each
    value=(left_border, right_border) is a tuple containing the borders of the window in seconds
    :param window_to_id_to_hle: a dict where first key is the window id, second key is the high-level event id and the
    value is the HLE (aspect, entity, component, traffic_type, value, window)
    :param cascade_dict: a dictionary assigning IDs to hle, such that two hle get the same ID iff they are directly or
    indirectly connected through the link values
    :param tz_info: UTC
    :param hla_filtered: list of high-level activities that should appear in the high-level log
    :param args: parameters only_entity and traffic_of_interest of class HlLogArgs
    :param flatten: if True, hle within the same window of the same cascade get slightly different ts. Otherwise, hle of
    same window of same cascade will have identical ts (= the window's left border)
    :return: the high-level event log in xes format, the high-level event log as pandas dataframe
    """
    log_df = create_dataframe(window_border_dict, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered, args, flatten)
    log_xes = df_to_xes_log(log_df)

    if export:
        current_dir = os.path.dirname(__file__)

        max_counter_xes = get_max_counter(file_type='.xes')
        log_name_xes = f"high_level_log_{max_counter_xes}.xes"
        path_xes = os.path.join(current_dir, log_name_xes)
        export_hl_log(path_xes, path_xes)

        max_counter_csv = get_max_counter(file_type='.csv')
        log_name_csv = f"high_level_log_{max_counter_csv}.csv"
        path_csv = os.path.join(current_dir, log_name_csv)
        log_df.to_csv(path_csv, index=False)

    return log_xes, log_df
