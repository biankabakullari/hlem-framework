import os
import pickle
import pandas as pd
import pm4py
from hl_paths.significance import significance


# return list of all resources except User1 which is non-human
def get_res_selection(log):
    res_list = []
    for trace in log:
        for event in trace:
            res = event['org:resource']
            res_list.append(res)
    user_1 = ['User_1']
    res_set = set(res_list).difference(set(user_1))
    return res_set


# keep only cases that are completed
# applying this function removes "only" 98 of the 31509 traces
def filter_incomplete_cases(log):
    act_list = ['A_Cancelled', 'A_Pending', 'A_Denied']
    filtered_log = pm4py.filter_event_attribute_values(log, "concept:name", act_list, level="case", retain=True)

    return filtered_log


# one may change the <= and < signs onto < and <=
def partition_on_throughput(log):
    class_under_10 = []
    class_10_to_30 = []
    class_over_30 = []
    for i, trace in enumerate(log):
        ts_first = trace[0]['time:timestamp']
        ts_last = trace[len(trace)-1]['time:timestamp']
        throughput = (ts_last - ts_first).days
        if throughput <= 10:
            class_under_10.append(i)
        elif throughput < 30:
            class_10_to_30.append(i)
        else:
            class_over_30.append(i)
    return class_under_10, class_10_to_30, class_over_30


# a case has been successful if it has activity A_Pending
def partition_on_outcome(log):
    successful = []
    unsuccessful = []
    for i, trace in enumerate(log):
        control_flow = [event['concept:name'] for event in trace]
        if 'A_Pending' in control_flow:
            successful.append(i)
        else:
            unsuccessful.append(i)
    return successful, unsuccessful


# a case has been successful if it has activity A_Pending
def partition_on_amount(log):
    under_10k = []
    over_10k = []
    for i, trace in enumerate(log):
        amount = trace.attributes['RequestedAmount']
        if amount <= 10000:
            under_10k.append(amount)
        else:
            over_10k.append(amount)
    return under_10k, over_10k


# For workflow activities, concatenate the lifecycle to the activity itself
def adjust_lifecycles(log):
    for trace in log:
        for event in trace:
            act = event['concept:name']
            if act.startswith('W'):
                lifecycle = event['lifecycle:transition']
                new_act = act + '|' + lifecycle
                event['concept:name'] = new_act
    return log


def preprocess_bpic_2017(my_path):
    cache_path = my_path.replace('.xes', '.pickle')
    if os.path.isfile(cache_path):
        with open(cache_path, 'rb') as f:
            log = pickle.load(f)
    else:
        log = pm4py.read_xes(my_path)
        with open(cache_path, 'wb') as f:
            pickle.dump(log, f)

    # filter incomplete cases
    log = filter_incomplete_cases(log)

    # attach lifecycle info to activity label
    log = adjust_lifecycles(log)

    # partition of cases using outcome
    successful, not_successful = partition_on_outcome(log)

    # partition of cases using throughput
    class_under_10, class_10_to_30, class_over_30 = partition_on_throughput(log)

    # partition based on loan amount
    under_10k, over_10k = partition_on_amount(log)

    # determine res_selected which excludes User_1
    resources = get_res_selection(log)

    return log, resources, successful, not_successful, class_under_10, class_10_to_30, class_over_30, under_10k, \
           over_10k


def success_tables(result_df, outcome_success):
    header = ['Length', 'Frequency', 'Path', 'Part&Succ', 'Part&NotSucc', 'NonPart&Succ', 'NonPart&NotSucc', 'p']
    rows = []
    for i in range(len(result_df)):
        path_i = result_df.iloc[i]
        path = path_i['path']
        path_freq = path_i['frequency']
        participating = path_i['participating']
        non_participating = path_i['non-participating']
        participation = [participating, non_participating]

        part_and_succ = len(participation[0].intersection(outcome_success[0]))
        part_and_non_succ = len((participation[0].intersection(outcome_success[1])))
        non_part_and_succ = len(participation[1].intersection(outcome_success[0]))
        non_part_and_non_succ = len((participation[1].intersection(outcome_success[1])))

        p_value, special_success = significance(participation, outcome_success)
        if special_success:
            row = [len(path), path_freq, path, part_and_succ, part_and_non_succ, non_part_and_succ,
                   non_part_and_non_succ, p_value]
            rows.append(row)

    eval_df = pd.DataFrame(rows, columns=header)
    file_name = 'success-2-classes' + '.csv'
    eval_df.to_csv(file_name, index=True, header=True)


def throughput_tables(result_df, outcome_throughput):
    header = ['Length', 'Frequency', 'Path', 'Part&under10', 'Part&10to30', 'Part&over30', 'NonPart&under10',
              'NonPart&10to30', 'NonPart&over30', 'p']
    rows = []
    for i in range(len(result_df)):
        path_i = result_df.iloc[i]
        path = path_i['path']
        path_freq = path_i['frequency']
        participating = path_i['participating']
        non_participating = path_i['non-participating']
        participation = [participating, non_participating]

        part_and_class1 = len(participation[0].intersection(outcome_throughput[0]))
        part_and_class2 = len(participation[0].intersection(outcome_throughput[1]))
        part_and_class3 = len(participation[0].intersection(outcome_throughput[2]))
        non_part_and_class1 = len(participation[1].intersection(outcome_throughput[0]))
        non_part_and_class2 = len(participation[1].intersection(outcome_throughput[1]))
        non_part_and_class3 = len(participation[1].intersection(outcome_throughput[2]))

        p_value, special_throughput = significance(participation, outcome_throughput)
        if special_throughput:
            row = [len(path), path_freq, path, part_and_class1, part_and_class2, part_and_class3, non_part_and_class1,
                   non_part_and_class2, non_part_and_class3, p_value]
            rows.append(row)

    eval_df = pd.DataFrame(rows, columns=header)
    file_name = 'throughput-3-classes' + '.csv'
    eval_df.to_csv(file_name, index=True, header=True)


def amount_tables(result_df, amount_partition):
    header = ['Length', 'Frequency', 'Path', 'Part&Succ', 'Part&NotSucc', 'NonPart&Succ', 'NonPart&NotSucc', 'p']
    rows = []
    for i in range(len(result_df)):
        path_i = result_df.iloc[i]
        path = path_i['path']
        path_freq = path_i['frequency']
        participating = path_i['participating']
        non_participating = path_i['non-participating']
        participation = [participating, non_participating]

        part_and_under10k = len(participation[0].intersection(amount_partition[0]))
        part_and_over10k = len((participation[0].intersection(amount_partition[1])))
        non_part_and_under10k = len(participation[1].intersection(amount_partition[0]))
        non_part_and_over10k = len((participation[1].intersection(amount_partition[1])))

        p_value, special_success = significance(participation, amount_partition)
        if special_success:
            row = [len(path), path_freq, path, part_and_under10k, part_and_over10k, non_part_and_under10k,
                   non_part_and_over10k, p_value]
            rows.append(row)

    eval_df = pd.DataFrame(rows, columns=header)
    file_name = 'amount-2-classes' + '.csv'
    eval_df.to_csv(file_name, index=True, header=True)


if __name__ == '__main__':
    _, resources, successful, not_successful, class_under_10, class_10_to_30, class_over_30, under_10k, \
           over_10k = preprocess_bpic_2017()
    total = len(successful)+len(not_successful)
    print('There are in total: ' + str(total) + ' cases.')
    print('There are in total: ' + str(len(resources)) + ' resources.')
    print('Successful: ' + str(len(successful)) + ' ' + '(' + str(round(len(successful)*100/total, 2)) + '%)')
    print('Not successful: ' + str(len(not_successful)) + ' ' + '(' + str(round(len(not_successful)*100 / total, 2)) +
          '%)')
    print('<= 10 days: ' + str(len(class_under_10)) + ' ' + '(' + str(round(len(class_under_10)*100 / total, 2)) + '%)')
    print('10-30 days: ' + str(len(class_10_to_30)) + ' ' + '(' + str(round(len(class_10_to_30)*100 / total, 2)) + '%)')
    print('> 30 days: ' + str(len(class_over_30)) + ' ' + '(' + str(round(len(class_over_30)*100 / total, 2)) + '%)')
