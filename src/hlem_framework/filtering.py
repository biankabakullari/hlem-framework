import numpy as np
from collections import defaultdict


def get_most_freq_segments(log, percentile):
    """
    :param log: the event log
    :param percentile: a number between 0 and 1
    :return:
    a list of activity pairs that directly follow each other which cover the given percentile of the directly follows
    pairs in the log
    """

    segment_freq_dic = defaultdict(lambda: 0)
    for trace in log:
        cf = [event['concept:name'] for event in trace]
        trace_segs = [(cf[i], cf[i+1]) for i in range(len(trace)-1)]
        for seg in trace_segs:
                segment_freq_dic[seg] += 1

    frequencies = list(segment_freq_dic.values())
    thresh = np.percentile(frequencies, percentile*100)
    selected_segments = [key for key in segment_freq_dic.keys() if segment_freq_dic[key] >= thresh]
    return selected_segments


def surviving_steps(log, percentile):
    """
    :param log: the event log
    :param percentile: a number between 0 and 1
    :return:
    a list of pairs of events that directly follow each other whose activity pairs cover the given percentile of
    the directly follows pairs in the log
    """
    selected_segments = get_most_freq_segments(log, percentile)

    surviving_pairs = []
    for trace_index, trace in enumerate(log):
        for i in range(len(trace)-1):
            ev_i = trace[i]
            ev_j = trace[i+1]
            seg = (ev_i['concept:name'], ev_j['concept:name'])
            if seg in selected_segments:
                surviving_pairs.append((ev_i, ev_j))
            # TODO: What does this do?
            surviving_pairs.append(())

    return surviving_pairs
