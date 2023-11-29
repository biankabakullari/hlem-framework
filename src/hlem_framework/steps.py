import itertools as it
import frames


def df_partial_order_trace(trace):
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
        p_i = to_partition[p]
        p_j = to_partition[p + 1]
        ij_pairs = [pair for pair in it.product(p_i, p_j)]
        df_indices.extend(ij_pairs)

    return df_indices


def df_total_order_trace(trace):
    """
    :param trace: a sequence of events
    :return: the indices of the event pairs that directly follow each other
    here: to=total order, so event pairs correspond to the order in which they appear in the sequence, without taking
    care of identical timestamps
    """

    n = len(trace)
    df_indices = [(i, i + 1) for i in range(n - 1)]
    assert len(df_indices) == n-1, 'Error in detecting the directly-follows event pairs'

    return df_indices


def directly_follows_pairs(trace):
    """
    :param trace: a sequence of events
    :return: a list of pairs (i,j) where events in positions i and j directly follow each other in the trace
    (identical timestamps are considered)
    """

    n = len(trace)
    if n == 1:
        return [(trace[0])]

    else:
        ts_set = set([frames.seconds_since_epoch(event['time:timestamp']) for event in trace])
        if len(ts_set) < n:  # trace contains less unique ts than events, so trace has events with identical ts
            df_indices = df_partial_order_trace(trace)

        else:  # each ts in the trace is unique, so trace is totally ordered
            df_indices = df_total_order_trace(trace)

        return df_indices


#TODO
# def mf(model, trace):


def log_steps_by_trace(log, method):
    """
    :param log: the event log
    :param method: the way steps should be defined (e.g. "df" for directly-follows or "mf" for model-follows)
    :return: a dictionary, each trace position in the log is the key, and attributes are its length and its df indices
    """
    trace_pairs_dict = {}
    if method != "df":
        pass
    else:
        for t_index, trace in enumerate(log):
            steps = directly_follows_pairs(trace)
            trace_pairs_dict[t_index] = {'length': len(trace), 'steps': steps}

    return trace_pairs_dict


def trigger_release_dicts(log, method):

    """
    :param log: the event log
    :param method: the way steps should be defined (e.g. "df" for directly-follows or "mf" for model-follows)
    :return:
    Given an event log and a method for defining steps, returns:
    - a list of pairs of numbers, each number uniquely identifies an event from the log, each pair constitutes a step
    - trigger dict: a dictionary where each key,value pair i: [j1,...,jn] means that set (i,j1),...,(i,jn) are steps
    - release dict: a dictionary where each key,value pair j: [i1,...,in] means that set (i1,j),...,(in,j) are steps
    """
    steps_by_trace = log_steps_by_trace(log, method)

    all_steps_list = []
    triggers_dict = {}
    releases_dict = {}

    pos = 0

    for trace_index in steps_by_trace.keys():

        trace_info = steps_by_trace[trace_index]
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
                all_steps_list.append((pos+i, pos+j))
                triggers_dict[pos + i].append(pos + j)
                releases_dict[pos + j].append(pos + i)
        pos += trace_length

    return all_steps_list, triggers_dict, releases_dict


if __name__ == '__main__':
    pass
