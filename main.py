import os.path
import hlem
import result_statistics as stat
import math

standard_list = frozenset(['exec', 'enter', 'wt', 'wl'])


def main(path, traffic_type='High', selected_f_list=standard_list, p=80, relative_congestion=True,
         connection_thresh=0, res_info=True, freq=0, only_comp=False, act_selection='all', res_selection='all',
         seg_method='df', flatten=False):

    log = stat.read_log(path)
    no_events = sum([len(trace) for trace in log])
    ts_first = log[0][0]['time:timestamp']
    last_trace = log[len(log)-1]
    ts_last = log[len(log)-1][len(last_trace)-1]['time:timestamp']
    seconds_total = (ts_last-ts_first).total_seconds()
    # no_windows = 6 * (seconds_total / 3600)  # used in the evaluation of the simulated log
    no_windows = 100*math.ceil(math.sqrt(no_events))
    hl_log, df = hlem.transform_log_to_hl_log_width(log, no_windows, traffic_type, selected_f_list, p,
                                                    relative_congestion,  connection_thresh, res_info, freq, only_comp,
                                                    act_selection, res_selection, seg_method, flatten)

    return hl_log


if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)
    my_path = os.path.join(current_dir, 'log.xes')
    main(path=my_path)
