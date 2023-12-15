from dataclasses import dataclass
import pandas as pd
import os
import re
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import pm4py
import frames
from frames import Frame
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


def create_hle_table(case_column, activity_column, timestamp_column, value_column, component_column, frame: Frame):
    """
    :param case_column: a list with (high-level) case ids as values
    :param activity_column: a list with (high-level) activity labels as values
    :param timestamp_column: a list with timestamps as values
    :param value_column: a list with numbers as values
    :param component_column: a list with components ('activity', 'segment', or 'resource') as values
    :return: a dataframe containing the input columns
    """
    table = pd.DataFrame(list(zip(case_column, activity_column, timestamp_column, value_column, component_column)),
                         columns=['case:concept:name', 'concept:name', 'time:timestamp', 'value', 'component'])

    if frame in ['minutes', 'hours', 'days', 'weeks']:
        #check if some high-level activity refers to delay
        delay_indices = table[table['concept:name'].str.contains('delay', case=True)].index
        divider = frames.get_window_size_from_unit(frame)
        table.loc[delay_indices, 'value'] /= divider

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
                     flatten, frame):
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
    :param frame: can be a number (determining number of windows) or a time unit (minutes, hours, days, or weeks)
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

    table = create_hle_table(case_column, act_column, ts_column, val_column, comp_type_column, frame)

    return table


def df_to_xes_log(df):
    """
    :param df: dataframe of high-level log
    :return: high-level log in xes format
    """
    #parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case'}
    #hl_event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
    hl_event_log = pm4py.convert_to_event_log(df)
    return hl_event_log


def export_hl_log(log, path):
    """
    :param log: input log
    :param path: the path in the dir where the log must be saved
    :return:
    """
    pm4py.write_xes(log, path)


def get_max_counters():
    xes_string = '.xes'
    csv_string = '.csv'
    hl_string = 'high_level_log'

    files = os.listdir()
    count_xes = 0
    count_csv = 0
    for filename in files:
        if hl_string in filename and xes_string in filename:
            count_xes += 1
        elif hl_string in filename and csv_string in filename:
            count_csv += 1
    return count_xes, count_csv


def generate_hl_xes_and_df(window_border_dict, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered,
                           args: HlLogArgs, flatten: bool, frame, export: bool):
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
    :param frame: can be a number (determining number of windows) or a time unit (minutes, hours, days, or weeks)
    :param export:
    :return: the high-level event log in xes format, the high-level event log as pandas dataframe
    """
    log_df = create_dataframe(window_border_dict, window_to_id_to_hle, cascade_dict, tz_info, hla_filtered, args,
                              flatten, frame)
    log_xes = df_to_xes_log(log_df)
    #print("Conversion from df to event log object works!")
    if export:

        current_dir = os.getcwd()
        print("current dir before output:", current_dir)

        output_path = os.path.join(current_dir, "output")
        os.chdir(output_path)
        print("current directory should be output:", os.getcwd())

        current_dir = os.getcwd()
        print("directory where the log will be added:", current_dir)

        xes_counter, csv_counter = get_max_counters()

        if xes_counter == 0:
            log_name_xes = f"high_level_log.xes"
        else:
            log_name_xes = f"high_level_log({xes_counter}).xes"

        path_xes = os.path.join(current_dir, log_name_xes)
        export_hl_log(log_xes, path_xes)

        if csv_counter == 0:
            log_name_csv = f"high_level_log.csv"
        else:
            log_name_csv = f"high_level_log({csv_counter}).csv"
        path_csv = os.path.join(current_dir, log_name_csv)
        log_df.to_csv(path_csv, index=False)

    return log_xes, log_df


def display_cascade_sequences(hl_log_df):
    unique_cascades = hl_log_df['case:concept:name'].unique()
    casc_count = hl_log_df['case:concept:name'].value_counts()

    for casc in unique_cascades:
        no_hle = casc_count[casc]
        casc_df = hl_log_df['case:concept:name'] == casc
        casc_ts = hl_log_df[casc_df]['time:timestamp'].tolist()
        no_ts = len(set(casc_ts))
        casc_df_sorted = hl_log_df[casc_df].sort_values('time:timestamp')
        hla_sorted = casc_df_sorted['concept:name'].tolist()

        if no_hle <= no_ts:  # no hle with same ts
            cascade_sequence = hla_sorted
        else:
            ts_sorted = casc_df_sorted['time:timestamp'].tolist()
            last_ts = ts_sorted[0]
            cascade_sequence = [{hla_sorted[0]}]
            hla_sorted = hla_sorted[1:]
            for i, hla in enumerate(hla_sorted):
                ts_i = ts_sorted[i + 1]
                if ts_i == last_ts:
                    last_hla_set = cascade_sequence[-1]
                    last_hla_set.add(hla)
                    cascade_sequence[-1] = last_hla_set
                else:
                    cascade_sequence.append({hla})
                    last_ts = ts_i

        print("Cascade ID: ", casc)
        print(cascade_sequence)


def relevant_hl_log_info(hl_log_df):
    # print some statistics
    no_hle = len(hl_log_df['concept:name'])
    no_unique_hla = len(hl_log_df['concept:name'].unique())
    print(f'No. high-level events: {no_hle}')
    print(f'No. unique high-level activities: {no_unique_hla}')
    no_cascades = len(hl_log_df['case:concept:name'].unique())
    print(f'No. cascades (high-level cases): {no_cascades}')

    # shows high-level activities and their counts from most to least frequent
    hla_counts = hl_log_df['concept:name'].value_counts(ascending=False)
    hla_counts_df = hla_counts.reset_index()
    hla_counts_df.columns = ['unique high-level activity', 'frequency in hl-log']
    print(hla_counts_df)

    # shows cascade size (no. events) and their count from largest to smallest
    cascade_no_events = list(hl_log_df['case:concept:name'].value_counts(ascending=False))
    casc_size_column = list(set(cascade_no_events))
    size_count_column = [cascade_no_events.count(casc_size) for casc_size in casc_size_column]
    casc_size_df = pd.DataFrame({
        'no. cascades with size': size_count_column,
        'cascade size': casc_size_column
    })
    print(casc_size_df)
    #display_cascade_sequences(hl_log_df)
