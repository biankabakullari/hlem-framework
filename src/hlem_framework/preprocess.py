import itertools as it
import numpy as np
import frames


def df_po_trace(trace):
    """

    :param trace: a sequence of events
    :return: the indices of the event pairs that directly follow each other
    here: po=partial order, so events with identical timestamps are taken into consideration
    """
    n = len(trace)
    df_indices = []

    to_partition = {0: [0]}  # totally ordered partition classes, first class contains the index of first event
    last_partition = 0
    last_ts = trace[0]['time:timestamp']
    for i in range(1, n):
        curr_ts = trace[i]['time:timestamp']
        if curr_ts == last_ts:
            to_partition[last_partition].append(i)
        else:
            last_partition += 1
            to_partition[last_partition] = [i]
            last_ts = curr_ts

    partitions = sorted(to_partition.keys())
    for p in range(len(partitions) - 1):
        p_class = to_partition[p]
        q_class = to_partition[p + 1]
        pq_pairs = [pair for pair in it.product(p_class, q_class)]
        df_indices.extend(pq_pairs)

    return df_indices


def df_to_trace(trace):
    """

    :param trace: a sequence of events
    :return: the indices of the event pairs that directly follow each other
    here: to=total order, so event pairs correspond to the order in which they appear in the sequence, without taking
    care of identical timestamps
    """
    n = len(trace)
    df_indices = [(i, i + 1) for i in range(n - 1)]

    return df_indices


def directly_follows(trace):
    """
    decides for a given trace, whether there are events with identical timestamps, and selects the correct method of
    computing the directly-follows pairs
    """
    n = len(trace)
    if n == 1:
        return [(trace[0])]

    else:
        ts_set = set([frames.seconds_since_epoch(event['time:timestamp']) for event in trace])
        if len(ts_set) < n:  # trace contains less unique ts than events, so trace has events with identical ts
            df_indices = df_po_trace(trace)

        else:  # each ts in the trace is unique, so trace is totally ordered
            df_indices = df_to_trace(trace)

        return df_indices


#TODO
# def mf(model, trace):


def log_steps(log, method):
    """
    :param log: the event log
    :param method: the way steps should be defined (e.g. "df" for directly-follows or "mf" for model-follows)
    :return: a dictionary, each trace position in the log is the key, and attributes are its length and its steps
    """
    trace_pairs_dict = {}
    if method != "df":
        pass
    else:
        for t_index, trace in enumerate(log):
            steps = directly_follows(trace)
            trace_pairs_dict[t_index] = {'length': len(trace), 'steps': steps}

    return trace_pairs_dict


def trig_rel_dicts(log, method):

    """
    Given an event log and a method for defining steps, returns:
    - a list of pairs of numbers, each number uniquely identifies an event from the log, each pair constitutes a step
    - a dictionary where each key,value pair i: [j1,...,jn] means that set (i,j1),...,(i,jn) are steps
    - a dictionary where each key,value pair j: [i1,...,in] means that set (j,i1),...,(j,in) are steps
    """
    steps = log_steps(log, method)

    steps_list = []
    triggers_dict = {}
    releases_dict = {}

    pos = 0

    for trace_index in steps.keys():

        trace_info = steps[trace_index]
        trace_length = trace_info['length']
        trace_steps = trace_info['steps']
        event_numbers = [i for i in range(trace_length)]
        if trace_length == 1:
            triggers_dict[pos] = []
            releases_dict[pos] = []
        else:
            for i in event_numbers:
                triggers_dict[pos + i] = []
                releases_dict[pos + i] = []
            for pair in trace_steps:
                i = pair[0]
                j = pair[1]
                steps_list.append((pos+i, pos+j))
                triggers_dict[pos + i].append(pos + j)
                releases_dict[pos + j].append(pos + i)
        pos += trace_length

    return steps_list, triggers_dict, releases_dict


def get_most_freq_segments(log, percentile):
    segment_freq_dic = dict()
    for trace in log:
        cf = [event['concept:name'] for event in trace]
        trace_segs = [(cf[i], cf[i+1]) for i in range(len(trace)-1)]
        for seg in trace_segs:
            if seg in segment_freq_dic.keys():
                segment_freq_dic[seg] += 1
            else:
                segment_freq_dic[seg] = 1

    frequencies = list(segment_freq_dic.values())
    thresh = np.percentile(frequencies, percentile*100)
    selected_segments = [key for key in segment_freq_dic.keys() if segment_freq_dic[key] >= thresh]
    return selected_segments


def surviving_steps(log, seg_percentile):
    selected_segments = get_most_freq_segments(log, seg_percentile)

    surviving_pairs = []
    for i, trace in enumerate(log):
        for j in range(len(trace)-1):
            ev_j = trace[j]
            ev_j_next = trace[j+1]
            seg = (ev_j['concept:name'], ev_j_next['concept:name'])
            if seg in selected_segments:
                surviving_pairs.append((ev_j, ev_j_next))
            surviving_pairs.append(())

    return surviving_pairs


def event_dic_with_resource(log):
    """
    returns a dictionary where each key is a number uniquely identifying some event from the log, the value is a
    dictionary containing the attribute values for the case, activity, timestamp, resource. The value of 'single' is
    True iff the event is the only one recorded for the corresponding trace.
    """
    event_dic = {}
    pos = 0
    for i, trace in enumerate(log):
        n = len(trace)
        #case = trace.attributes['concept:name']
        case = i
        for j in range(n):
            event = trace[j]
            act = event['concept:name']
            ts = event['time:timestamp']
            ts_seconds = frames.seconds_since_epoch(ts)
            res = event['org:resource']
            event_dic[pos + j] = {'case': case, 'act': act, 'ts': ts, 'ts-seconds': ts_seconds, 'res': res,
                                  'single': False}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dic_wo_resource(log):
    """
    returns a dictionary where each key is a number uniquely identifying some event from the log, the value is a
    dictionary containing the attribute values for the activity, timestamp. The value of 'single' is True iff
    the event is the only one recorded for the corresponding trace
    """
    event_dic = {}
    pos = 0
    for i, trace in enumerate(log):
        n = len(trace)
        #case = trace.attributes['concept:name']
        case = i
        for j in range(n):
            event = trace[j]
            act = event['concept:name']
            ts = event['time:timestamp']
            ts_seconds = frames.seconds_since_epoch(ts)
            event_dic[pos + j] = {'case': case, 'act': act, 'ts': ts, 'ts-seconds': ts_seconds, 'single': False}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dict(log, res_info):
    """

    :param log: the event log
    :param res_info: must be set to False if there is no resource information in the event log
    :return: a dictionary with unique integers identifying events, and their attribute values as value
    """
    if res_info:
        return event_dic_with_resource(log)
    else:
        return event_dic_wo_resource(log)


