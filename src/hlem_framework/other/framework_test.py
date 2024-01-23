import os.path
from preprocess import ProjectLogArgs
from hlem_with_log import transform_log_to_hl_log, HlemArgs
from hl_log.hl_log import relevant_hl_log_info
import pm4py
import pandas as pd


def test_only_resource(log, export_desired: bool):
    projection_args = ProjectLogArgs()
    hlem_args = HlemArgs(aspects=['do', 'todo', 'busy'])
    log_xes, log_df = transform_log_to_hl_log(log, act_selection=[], res_selection='all', seg_selection=[],
                                              args=hlem_args, project_args=projection_args, export=export_desired)
    relevant_hl_log_info(log_df)
    return log_xes, log_df


def test_traffic_types(log, export_desired: bool):
    projection_args = ProjectLogArgs()

    hlem_args_low = HlemArgs(traffic_of_interest='low')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_low, project_args=projection_args, export=export_desired)
    print('Low traffic succeeded')
    #relevant_hl_log_info(log_df)

    hlem_args_high = HlemArgs(traffic_of_interest='high')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_high, project_args=projection_args, export=export_desired)
    print('High traffic succeeded')
    #relevant_hl_log_info(log_df)

    hlem_args_low_high = HlemArgs(traffic_of_interest='low and high')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_low_high, project_args=projection_args, export=export_desired)
    print('Both traffic types succeeded')
    #relevant_hl_log_info(log_df)


def run_default(log, export_desired):
    hlem_args_default = HlemArgs()
    projection_args = ProjectLogArgs()
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_default, project_args=projection_args,
                                              export=export_desired)

    relevant_hl_log_info(log_df)
    return log_xes, log_df


if __name__ == '__main__':
    os.chdir("../")
    os.chdir("../")
    current = os.getcwd()
    my_path = os.path.join(current, "event_logs", "running-example.xes")
    log = pm4py.read_xes(my_path)
    #log_xes, log_df = run_default(log=log, export_desired=True)
    log_xes, log_df = test_only_resource(log=log, export_desired=True)
    #for index, row in log_df.iterrows():
        #print(row)
    #test_traffic_types(log, export_desired=False)

    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.width', None)  # Use maximum width for displaying each row
    pd.set_option('display.max_colwidth', None)
    print(log_df)

