import frames


def event_dic_with_resource(log):
    """
    :param log: the event log
    :return: a dictionary where each key is a number uniquely identifying some event from the log, the value is a
    dictionary containing the attribute values for the case, activity, timestamp, resource. The value of 'single' is
    True iff the event is the only one recorded for the corresponding trace.
    """
    event_dic = {}
    pos = 0
    for trace_index, trace in enumerate(log):
        n = len(trace)
        #case = trace.attributes['concept:name']
        case = trace_index

        for j in range(n):
            event = trace[j]
            act = event['concept:name']

            ts = event['time:timestamp']
            ts_seconds = frames.seconds_since_epoch(ts)
            # TODO: check if this still works with negative seconds
            assert ts_seconds > 0, 'Event happened before January 1st, 1970!'

            res = event['org:resource']
            if res == '':
                res = 'UNK'

            event_dic[pos + j] = {'case': case, 'act': act, 'ts': ts, 'ts-seconds': ts_seconds, 'res': res,
                                  'single': False}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dic_wo_resource(log):
    # TODO: change to named tuples
    """
    :param log: the event log
    returns a dictionary where each key is a number uniquely identifying some event from the log, the value is a
    dictionary containing the attribute values for the activity, timestamp. The value of 'single' is True iff
    the event is the only one recorded for the corresponding trace
    """
    event_dic = {}
    pos = 0
    for i, trace in enumerate(log):
        n = len(trace)
        case = i
        for j in range(n):
            event = trace[j]
            act = event['concept:name']
            ts = event['time:timestamp']
            ts_seconds = frames.seconds_since_epoch(ts)
            # TODO: check if this still works with negative seconds
            assert ts_seconds > 0, 'Event happened before January 1st, 1970!'
            event_dic[pos + j] = {'case': case, 'act': act, 'ts': ts, 'ts-seconds': ts_seconds, 'single': False}
        if n == 1:
            event_dic[pos]['single'] = True
        pos += n

    return event_dic


def event_dict(log, res_info):
    """

    :param log: the event log
    :param res_info: default is False, if True, resource information is collected
    :return: a dictionary where each key is a unique identifier of an event (natural number), the value is a dict with
    each attribute as key and attribute value as value
    """
    if res_info:
        return event_dic_with_resource(log)
    else:
        return event_dic_wo_resource(log)
