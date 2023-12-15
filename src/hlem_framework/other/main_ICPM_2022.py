from hlem_with_log import transform_log_to_hl_log, HlemArgs
from hl_log.hl_log import relevant_hl_log_info
import pm4py
import os


def run_default_parameters(log, export_desired):
    hlem_args_default = HlemArgs()
    log_xes, log_df = transform_log_to_hl_log(log, act_selection=0.8, res_selection=0.8, seg_selection=0.8,
                                              args=hlem_args_default, export=export_desired)

    relevant_hl_log_info(log_df)
    return log_xes, log_df


def reproduce_results_simulated_log():
    pass


def reproduce_results_real_log():
    pass


if __name__ == '__main__':
    print(os.getcwd())
    os.chdir("../")
    print(os.getcwd())
    os.chdir("../")
    os.chdir("../")
    print(os.getcwd())
    current = os.getcwd()
    my_path = os.path.join(current, "event_logs", "running-example.xes")
    log = pm4py.read_xes(my_path)
    run_default_parameters(log, export_desired=True)
