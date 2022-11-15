import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import preprocess


def create_hle_table(case_column, activity_column, timestamp_column, val_column, comp_type_column):
    table = pd.DataFrame(list(zip(case_column, activity_column, timestamp_column, val_column, comp_type_column)),
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

    for w in bucket_w_dict.keys():
        w_time_int = bucket_w_dict[w][0]
        w_time_ts = preprocess.int_to_ts(w_time_int, tz_info)
        hle_w = hle_all_w[w]
        for i, hle in enumerate(hle_w):
            hla = hle[:3]
            if hla in hla_filtered:
                case_id = cascade_dict[(w, i)]
                hla_string = hla_to_string(hla, only_component)
                val = hle[3]
                comp_type = hle[4]
                case_column.append(case_id)
                activity_column.append(hla_string)
                timestamp_column.append(w_time_ts)
                value_column.append(val)
                comp_type_column.append(comp_type)

    return case_column, activity_column, timestamp_column, value_column, comp_type_column


def create_dataframe(bucket_w_dict, congestion_all_w, cascade_dict, tz_info, hla_filtered, only_component):

    case_column, act_column, ts_column, val_column, comp_type_column = get_table_data(bucket_w_dict, congestion_all_w,
                                                                                      cascade_dict, tz_info,
                                                                                      hla_filtered, only_component)
    table = create_hle_table(case_column, act_column, ts_column, val_column, comp_type_column)

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
