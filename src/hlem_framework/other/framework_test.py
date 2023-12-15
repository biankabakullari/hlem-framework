import os.path
from hlem_with_log import transform_log_to_hl_log, HlemArgs
from hl_log.hl_log import relevant_hl_log_info
import pm4py


def test_only_resource(log, export_desired: bool):
    hlem_args = HlemArgs(aspects=['do', 'todo', 'busy'])
    log_xes, log_df = transform_log_to_hl_log(log, act_selection=[], res_selection='all', seg_selection=[],
                                              args=hlem_args, export=export_desired)
    relevant_hl_log_info(log_df)
    return log_xes, log_df


def test_traffic_types(log, export_desired: bool):
    hlem_args_low = HlemArgs(traffic_of_interest='low')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_low, export=export_desired)
    print('Low traffic succeeded')
    #relevant_hl_log_info(log_df)

    hlem_args_high = HlemArgs(traffic_of_interest='high')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_high, export=export_desired)
    print('High traffic succeeded')
    #relevant_hl_log_info(log_df)

    hlem_args_low_high = HlemArgs(traffic_of_interest='low and high')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_low_high, export=export_desired)
    print('Both traffic types succeeded')
    #relevant_hl_log_info(log_df)


def run_default(log, export_desired):
    hlem_args_default = HlemArgs()
    log_xes, log_df = transform_log_to_hl_log(log, act_selection=0.7, res_selection='all', seg_selection=0.5,
                                              args=hlem_args_default, export=export_desired)

    relevant_hl_log_info(log_df)
    return log_xes, log_df


if __name__ == '__main__':
    os.chdir("../")
    os.chdir("../")
    current = os.getcwd()
    my_path = os.path.join(current, "event_logs", "running-example.xes")
    log = pm4py.read_xes(my_path)
    log_xes, log_df = run_default(log=log, export_desired=True)
    #log_xes, log_df = test_only_resource(log=log, export_desired=True)
    for index, row in log_df.iterrows():
        print(row)
    #test_traffic_types(log, export_desired=False)


