import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import extractor


def create_hle_table(case_column, activity_column, timestamp_column, value_column, comp_type_column):
    table = pd.DataFrame(list(zip(case_column, activity_column, timestamp_column, value_column, comp_type_column)),
                         columns=['case', 'concept:name', 'time:timestamp', 'value', 'entity'])

    return table


def hla_to_string(hla_triple, only_component):
    hla_string_list = []

    f, comp, typ = hla_triple[0], hla_triple[1], hla_triple[2]
    if comp == 5:
        comp = 'Jane'

    if not isinstance(comp, int) and len(comp) == 2:
        s1 = comp[0]
        s2 = comp[1]
        comp_string = '(' + str(s1) + ',' + str(s2) + ')'
    else:
        comp_string = str(comp)

    if not only_component:
        f_string = str(f)
        if f_string == 'wt':
            f_string = 'delay'
        hla_string_list.append(f_string)
        typ_string = str(typ)
        #hla_string_list.append(typ_string)
    hla_string_list.append(comp_string)
    hla_string = '-'.join([string for string in hla_string_list])

    return hla_string


def get_table_data(bucket_w_dict, hle_all_w, cascade_dict, tz_info, hla_filtered, only_component):

    case_column = []
    activity_column = []
    timestamp_column = []
    value_column = []
    comp_type_column = []

    time_granularity_int = bucket_w_dict[0][1] - bucket_w_dict[0][0]  # window width
    for w in bucket_w_dict.keys():
        w_time_int = bucket_w_dict[w][0]
        hle_w_all = hle_all_w[w]
        hle_w = [hle for hle in hle_w_all if hle[:3] in hla_filtered]
        if len(hle_w) == 1:
            hle_0 = hle_w[0]
            hla_0 = hle_0[:3]
            case_id = cascade_dict[(w, 0)]
            hla_string = hla_to_string(hla_0, only_component)
            val = hle_0[3]
            comp_type = hle_0[4]
            case_column.append(case_id)
            activity_column.append(hla_string)
            w_time_ts = extractor.int_to_ts(w_time_int, tz_info)
            timestamp_column.append(w_time_ts)
            value_column.append(val)
            comp_type_column.append(comp_type)
        else:
            hla_strings = [hla_to_string(hle[:3], only_component) for hle in hle_w]
            hla_strings.sort()
            hla_strings.reverse()
            number = len(hla_strings)
            for i, hle in enumerate(hle_w):
                case_id = cascade_dict[(w, i)]
                case_column.append(case_id)
                activity_column.append(hla_strings[i])
                # if there are 5 events, they will get timestamps start + 0, ,...., start + 4/5*window_width
                w_hla_int = w_time_int + ((i/number)*time_granularity_int)
                w_time_ts = extractor.int_to_ts(w_hla_int, tz_info)
                timestamp_column.append(w_time_ts)
                val = hle[3]
                comp_type = hle[4]
                value_column.append(val)
                comp_type_column.append(comp_type)

    return case_column, activity_column, timestamp_column, value_column, comp_type_column


def create_dataframe(bucket_w_dict, congestion_all_w, cascade_dict, tz_info, hla_filtered, only_component):

    case_column, act_column, ts_column, val_column, comp_type_column = get_table_data(bucket_w_dict, congestion_all_w,
                                                                                      cascade_dict, tz_info,
                                                                                      hla_filtered, only_component)
    table = create_hle_table(case_column, act_column, ts_column, val_column, comp_type_column)

    table.to_csv('C:/Users/bakullari/Documents/Event logs/HL_Event_Log_local.csv', index=False)
    return table


def convert_to_event_log(df):

    parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case'}
    hl_event_log = log_converter.apply(df, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

    return hl_event_log


def export_hl_event_log(log):
    xes_exporter.apply(log, 'C:/Users/bakullari/Documents/Event logs/HL_Event_Log_local.xes')


def generate_hl_log(bucket_w_dict, congestion_all_w, cascade_dict, tz_info, hla_filtered, only_component):
    df = create_dataframe(bucket_w_dict, congestion_all_w, cascade_dict, tz_info, hla_filtered, only_component)
    hl_log = convert_to_event_log(df)

    export_hl_event_log(hl_log)

    return hl_log, df
