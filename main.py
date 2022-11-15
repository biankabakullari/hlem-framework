import os.path
import hlem
import pm4py
import math


DEFAULT_HLF = frozenset(['exec', 'enter', 'wt', 'wl'])

# path: the local path to the log data
# traffic_type: can be 'High', 'Low' or ['High', 'Low']
# hlf_selected: the list of selected high-level features, can be any non-empty list of elements from
# ['exec', 'todo', 'wl', 'enter', 'exit', 'progress', 'wt']
# p: the percentile to determine what is considered high (or low), a number 50 < p < 100
# relative_congestion:
# connection_thresh: the lambda value that determines whether any two hle are correlated or not, can be within [0,1]
# res_info: set to False if the event log has no resource information, otherwise can be set to True
# freq: a number within [0,1], the high-level log is projected onto the freq*100% most frequent high-level activities
# only_comp: if True, the high-level activity only shows the component involved (e.g. 'Jane' instead of 'wl-Jane')
# act_selection: if 'all', then all activities (and segments) in the initial log will be analyzed in combination with
# the chosen features (e.g. 'exec', 'enter'). Otherwise, it must be a list of the activities of interest, then only the
# activities of interest and the segments comprised of them will be analyzed with the chosen features
# res_selection: if 'all', then all resources in the initial log will be analyzed in combination with the chosen
# features (e.g. 'wl'), otherwise it must be a list of the resources of interest. If no resource information is present
# in the initial log, then assign 'all'.
# seg_method: atm, only 'df' (directly-follows) possible for determining the steps set
# flatten: if False, the high-level traces in the output log may contain high-level events with identical timestamps, if
# set to True, an artificial total order is introduced to flatten the log (here: a lexicographical order on the set of
# high-level activity labels)


def main(path, traffic_type='High', hlf_selected=DEFAULT_HLF, p=80, relative_congestion=True,
         connection_thresh=0, res_info=True, freq=0, only_comp=False, act_selection='all', res_selection='all',
         seg_method='df', flatten=False):
    """

    :param path:
    :param traffic_type:
    :param hlf_selected:
    :param p:
    :param relative_congestion:
    :param connection_thresh:
    :param res_info:
    :param freq:
    :param only_comp:
    :param act_selection:
    :param res_selection:
    :param seg_method:
    :param flatten:
    :return:
    """
    log = pm4py.read_xes(path)
    no_events = sum([len(trace) for trace in log])
    ts_first = log[0][0]['time:timestamp']
    last_trace = log[len(log)-1]
    ts_last = log[len(log)-1][len(last_trace)-1]['time:timestamp']
    seconds_total = (ts_last-ts_first).total_seconds()
    # no_windows = 6 * (seconds_total / 3600)  # used in the evaluation of the simulated log
    no_windows = math.ceil(math.sqrt(no_events))
    hl_log, df = hlem.transform_log_to_hl_log_width(log, no_windows, traffic_type, hlf_selected, p,
                                                    relative_congestion,  connection_thresh, res_info, freq, only_comp,
                                                    act_selection, res_selection, seg_method, flatten)

    return hl_log


if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)
    my_path = os.path.join(current_dir, 'running-example.xes')
    main(path=my_path)
