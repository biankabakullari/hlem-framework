import pandas as pd
import os
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import frames


def create_hle_table(case_column, activity_column, timestamp_column, value_column, comp_type_column):
    table = pd.DataFrame(list(zip(case_column, activity_column, timestamp_column, value_column, comp_type_column)),
                         columns=['case', 'concept:name', 'time:timestamp', 'value', 'entity'])

    return table


def hla_to_string(hla_triple, only_component):
    hla_string_list = []

    measure, entity, traffic_type = hla_triple[0], hla_triple[1], hla_triple[2]
    #if entity == 5:
        #comp = 'Jane'
    if not isinstance(entity, int) and len(entity) == 2:
        s1 = entity[0]
        s2 = entity[1]
        entity_string = '(' + str(s1) + ',' + str(s2) + ')'
    else:
        entity_string = str(entity)

    if not only_component:
        measure_string = str(measure)
        #if measure_string == 'wt':
            #measure_string = 'delay'
        hla_string_list.append(measure_string)
        traffic_string = str(traffic_type)
        hla_string_list.append(traffic_string)
    hla_string_list.append(entity_string)
    hla_string = '-'.join([string for string in hla_string_list])

    return hla_string


def get_table_data_po(window_border_dict, hle_all_by_theta, cascade_dict, tz_info, hla_filtered, only_component):

    case_column = []
    activity_column = []
    timestamp_column = []
    value_column = []
    comp_type_column = []

    for w in window_border_dict.keys():
        w_start_int = window_border_dict[w][0]
        w_start_ts = frames.seconds_to_datetime(w_start_int, tz_info)
        hle_w = hle_all_by_theta[w]
        for hle_id in hle_w.keys():
            hle = hle_w[hle_id]
            hla = (hle['f-type'], hle['entity'], hle['class'])  # (measure, entity, traffic type)
            if hla in hla_filtered:
                case_id = cascade_dict[(w, hle_id)]
                hla_string = hla_to_string(hla, only_component)
                val = hle['value']
                comp_type = hle['component']
                case_column.append(case_id)
                activity_column.append(hla_string)
                timestamp_column.append(w_start_ts)
                value_column.append(val)
                comp_type_column.append(comp_type)

    return case_column, activity_column, timestamp_column, value_column, comp_type_column


def get_table_data_flat(window_border_dict, hle_all_by_theta, cascade_dict, tz_info, hla_filtered, only_component):

    case_column = []
    activity_column = []
    timestamp_column = []
    value_column = []
    comp_type_column = []

    time_granularity_int = window_border_dict[0][1] - window_border_dict[0][0]  # window width
    windows = list(window_border_dict.keys())
    # print(window_border_dict)
    for w in windows:
        left_border_in_seconds = window_border_dict[w][0]
        hle_w_all = hle_all_by_theta[w]
        hle_ids_w_filtered = [hle_id for hle_id in hle_w_all.keys() if (hle_w_all[hle_id]['f-type'],
                                                                        hle_w_all[hle_id]['entity'],
                                                                        hle_w_all[hle_id]['class']) in hla_filtered]
        if len(hle_ids_w_filtered) == 1:
            id_0 = hle_ids_w_filtered[0]
            hle_0 = hle_w_all[id_0]
            hla_0 = (hle_0['f-type'], hle_0['entity'], hle_0['class'])
            case_id = cascade_dict[(w, id_0)]
            hla_string = hla_to_string(hla_0, only_component)
            val = hle_0['value']
            comp_type = hle_0['component']
            case_column.append(case_id)
            activity_column.append(hla_string)
            w_time_ts = frames.seconds_to_datetime(left_border_in_seconds, tz_info)
            timestamp_column.append(w_time_ts)
            value_column.append(val)
            comp_type_column.append(comp_type)
        else:
            hle_w_filtered = [hle_w_all[hle_id] for hle_id in hle_ids_w_filtered]
            hla_triples = [(hle['f-type'], hle['entity'], hle['class']) for hle in hle_w_filtered]
            hla_strings = [hla_to_string(hla, only_component) for hla in hla_triples]
            hla_strings.sort()
            # hla_strings.reverse()
            number = len(hla_strings)
            for i, hle_id in enumerate(hle_ids_w_filtered):
                case_id = cascade_dict[(w, hle_id)]
                case_column.append(case_id)
                activity_column.append(hla_strings[i])
                # if there are 5 events, they will get timestamps start + 0, ,...., start + 4/5*window_width
                w_hla_int = left_border_in_seconds + ((i/number)*time_granularity_int)
                w_time_ts = frames.seconds_to_datetime(w_hla_int, tz_info)
                timestamp_column.append(w_time_ts)
                hle = hle_w_all[hle_id]
                val = hle['value']
                comp_type = hle['component']
                value_column.append(val)
                comp_type_column.append(comp_type)

    return case_column, activity_column, timestamp_column, value_column, comp_type_column


def create_dataframe(window_border_dict, hle_all, cascade_dict, tz_info, hla_filtered, only_component, flatten):

    if flatten:
        case_column, act_column, ts_column, val_column, comp_type_column = get_table_data_flat(window_border_dict,
                                                                                               hle_all, cascade_dict,
                                                                                               tz_info, hla_filtered,
                                                                                               only_component)
    else:
        case_column, act_column, ts_column, val_column, comp_type_column = get_table_data_po(window_border_dict,
                                                                                             hle_all, cascade_dict,
                                                                                             tz_info, hla_filtered,
                                                                                             only_component)
    table = create_hle_table(case_column, act_column, ts_column, val_column, comp_type_column)

    # table.to_csv(path, index=False)
    return table


def convert_to_event_log(df):

    parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case'}
    hl_event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

    return hl_event_log


def export_hl_event_log(log, path):
    xes_exporter.apply(log, path)


def generate_hl_log(window_border_dict, hle_all, cascade_dict, tz_info, hla_filtered, only_component, flatten):
    df = create_dataframe(window_border_dict, hle_all, cascade_dict, tz_info, hla_filtered, only_component, flatten)
    hl_log = convert_to_event_log(df)

    current_dir = os.path.dirname(__file__)
    path = os.path.join(current_dir, 'high-level-log.xes')
    export_hl_event_log(hl_log, path)

    return hl_log, df
