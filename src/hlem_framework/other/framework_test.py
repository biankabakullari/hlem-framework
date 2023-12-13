import os.path
from hlem_with_log import transform_log_to_hl_log, HlemArgs
import pm4py
import math

# log, act_selection, seg_selection, res_selection, args: HlemArgs


def test_traffic_types(log, export_desired: bool):
    hlem_args_low = HlemArgs(traffic_of_interest='low')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_low, export=export_desired)

    hlem_args_high = HlemArgs(traffic_of_interest='high')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_high, export=export_desired)

    hlem_args_low_high = HlemArgs(traffic_of_interest='low and high')
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_low_high, export=export_desired)


def run_default(log, export_desired):
    hlem_args_default = HlemArgs()
    log_xes, log_df = transform_log_to_hl_log(log, act_selection='all', res_selection='all', seg_selection='all',
                                              args=hlem_args_default, export=export_desired)

    return log_df


def test_resource_config(log, export_desired: bool):
    res_selection = ['Mike', 'Paul']
    act_selection = []
    seg_selection = []
    aspects = ['do', 'todo', 'busy']
    # hlem_args = HlemArgs(aspects=aspects)
    pass


if __name__ == '__main__':
    #current_dir = os.path.dirname(__file__)
    #print("current_dir:", current_dir)
    #print("Current directory:", os.path.abspath(os.curdir))
    os.chdir("..")
    os.chdir("..")
    #print("Other directory:", os.path.abspath(os.curdir))
    current_dir = os.path.abspath(os.curdir)
    my_path = os.path.join(current_dir, "event_logs/running-example.xes")
    #print("my_path:", my_path)
    run_default(log=my_path, export_desired=True)
