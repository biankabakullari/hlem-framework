from datetime import datetime, timezone
import itertools as it


def ts_to_int(datetime_ts):
    if isinstance(datetime_ts, str):
        datetime_ts = datetime.strptime(datetime_ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    int_ts = (datetime_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    return int_ts


# yields the directly-follows index pairs by taking identical timestamps into consideration
def directly_follows_po_trace(trace):
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


# yields directly-follows pairs by assuming that no two events in the same trace have identical timestamps
def directly_follows_to_trace(trace):
    n = len(trace)
    df_indices = [(i, i + 1) for i in range(n - 1)]

    return df_indices


def directly_follows(trace):

    n = len(trace)
    if n == 1:
        return [(trace[0])]

    else:
        ts_set = set([ts_to_int(event['time:timestamp']) for event in trace])
        if len(ts_set) < n:  # trace contains less unique ts than events, so trace has events with identical ts
            df_indices = directly_follows_po_trace(trace)

        else:  # each ts in the trace is unique, so trace is totally ordered
            df_indices = directly_follows_to_trace(trace)

        return df_indices


#TODO
# def mf(model, trace):


def log_steps(log, method):
    trace_pairs_dict = {}
    if method == "mf":
        pass
    else:
        for t_index, trace in enumerate(log):
            steps = directly_follows(trace)
            trace_pairs_dict[t_index] = {'length': len(trace), 'steps': steps}

    return trace_pairs_dict


def trig_rel_dicts(log, method):
    steps = log_steps(log, method)

    pairs_list = []
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
                pairs_list.append((pos+i, pos+j))
                triggers_dict[pos + i].append(pos + j)
                releases_dict[pos + j].append(pos + i)
        pos += trace_length

    return pairs_list, triggers_dict, releases_dict


def event_dic_with_resource(log):
    event_dic = {}
    pos = 0
    for trace in log:
        n = len(trace)
        for i in range(n):
            event = trace[i]
            act = event['concept:name']
            ts = ts_to_int(event['time:timestamp'])
            res = event['org:resource']
            event_dic[pos + i] = {'act': act, 'ts': ts, 'res': res, 'single': False}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dic_wo_resource(log):
    event_dic = {}
    pos = 0
    for trace in log:
        n = len(trace)
        for i in range(n):
            event = trace[i]
            act = event['concept:name']
            ts = ts_to_int(event['time:timestamp'])
            event_dic[pos + i] = {'act': act, 'ts': ts, 'single': False}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dict(log, res_info):
    if res_info:
        return event_dic_with_resource(log)
    else:
        return event_dic_wo_resource(log)





def int_to_ts(int_number, tz_info):
    timestamp = datetime.fromtimestamp(int_number, tz_info)
    return timestamp
