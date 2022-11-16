import os.path
import hlem
import pm4py
import math


DEFAULT_HLF = frozenset(['exec', 'enter', 'wt', 'wl'])


def main(path, traffic_type='High', hlf_selected=DEFAULT_HLF, p=80, connection_thresh=0, res_info=True, freq=0,
         only_comp=False, act_selection='all', res_selection='all', seg_method='df', flatten=False):
    """

    :param path: the local path to the log data
    :param traffic_type: can be 'High', 'Low' or ['High', 'Low']
    :param hlf_selected: the list of selected high-level features, can be any non-empty list of elements from
    ['exec', 'todo', 'wl', 'enter', 'exit', 'progress', 'wt']
    :param p: the percentile to determine what is considered high (or low), a number 50 < p < 100
    :param connection_thresh: the lambda value in [0,1], determines whether any two hle are correlated or not
    :param res_info: must be set to False if the event log has no resource information, otherwise can be set to True
    :param freq: the threshold for selecting the most frequent high-level activities
    :param only_comp: if True, the high-level activity only shows the component involved (e.g. 'Jane' instead of
    'wl-Jane')
    :param act_selection: if 'all', then all activities (and segments) in the initial log will be analyzed in
    combination with the chosen measures (e.g. 'exec', 'enter'). Otherwise, it must be a list of the activities of
    interest, then only the activities of interest and the segments comprised of them will be analyzed with the chosen
    measures
    :param res_selection: if 'all', then all resources in the initial log will be analyzed in combination with the
    chosen measures (e.g. 'wl'). Otherwise, it must be a list of the resources of interest. If no resource information
    is present in the initial log, then assign 'all'.
    :param seg_method: currently, only 'df' (directly-follows) possible for determining the steps set
    :param flatten: If False, the high-level traces in the output log may contain high-level events with identical
    timestamps. If True, an artificial total order is introduced to flatten the log (here: a lexicographical order on
    the set of the high-level activity labels)
    :return: a (high-level) event log
    """
    log = pm4py.read_xes(path)
    no_events = sum([len(trace) for trace in log])
    ts_first = log[0][0]['time:timestamp']
    last_trace = log[len(log)-1]
    ts_last = log[len(log)-1][len(last_trace)-1]['time:timestamp']
    seconds_total = (ts_last-ts_first).total_seconds()
    #no_windows = 6 * (seconds_total / 3600)  # used in the evaluation of the simulated log
    no_windows = math.ceil(math.sqrt(no_events))
    hl_log, df = hlem.transform_log_to_hl_log_width(log, no_windows, traffic_type, hlf_selected, p, connection_thresh,
                                                    res_info, freq, only_comp, act_selection, res_selection, seg_method,
                                                    flatten)

    return hl_log


if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)
    my_path = os.path.join(current_dir, 'simulation.xes')
    main(path=my_path)
