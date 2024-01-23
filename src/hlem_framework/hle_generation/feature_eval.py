# import collections

def init_eval_hlf(all_activities, all_resources, all_segments, frames, act_selected, res_selected, features_selected):
    """
    :param all_activities: the set of all activities in the log
    :param all_resources: the set of all resources in the log
    :param all_segments: the set of all directly follows activity pairs in the log
    :param frames: a list of numbers 0,1,2,..., each uniquely identifying a window
    :param act_selected: the set of selected activities for analysis
    :param res_selected: the set of selected resources for analysis
    :param features_selected: the set of selected features for analysis (e.g., 'enter', 'delay', 'wl', ...)
    :return:
    -   eval_init: a dict with first level key value pairs: window id, dictionary for that window, and second level key
    value pairs: {(enqueue,a):0, (enqueue,b):0,...}, {(enter,(a,b)):0, (enter,(c,d)):0,...}, {(busy,r1):0, (busy,r2)...}
    -   A: the set of activities that qualify for analysis, determined by act_selected
    -   R: the set of resources that qualify for analysis, determined by res_selected
    -   S: the set of segments that qualify for analysis, determined by act_selected
        (those whose both underlying activities are in A)
    """
    eval_init = {frame: {} for frame in frames}
    hlf_init = []

    if act_selected == 'all':
        A = all_activities
        S = all_segments
    else:
        A = act_selected
        S = [(s1, s2) for (s1, s2) in all_segments if s1 in A and s2 in A]

    if 'dequeue' in features_selected:
        # counts the instances executing a at a given frame
        dequeue_a = {('dequeue', a): 0 for a in A}
        hlf_init.append(dequeue_a)
    if 'enqueue' in features_selected:
        # counts the instances enqueuing for a at a given frame (the last previous event happened at frame)
        enqueue_a = {('enqueue', a): 0 for a in A}
        hlf_init.append(enqueue_a)
    if 'queue' in features_selected:
        # counts the instances in queue waiting for a at the given frame
        # either enqueued in this frame, or some frame before
        queue_a = {('queue', a): 0 for a in A}
        hlf_init.append(queue_a)

    R = res_selected
    if res_selected == 'all':
        R = all_resources
    if len(R) > 0:
        if 'do' in features_selected:
            # counts the number of events resource r executes within the given frame
            do_r = {('do', r): 0 for r in R}
            hlf_init.append(do_r)
        if 'todo' in features_selected:
            # counts the number of events added to the task list of resource r executes within the given frame
            # (the last previous event happened at frame)
            todo_r = {('todo', r): 0 for r in R}
            hlf_init.append(todo_r)
        if 'busy' in features_selected:
            # counts the number of events in the task list of resource r at the given frame
            # either added in this frame, or some frame before
            busy_r = {('busy', r): 0 for r in R}
            hlf_init.append(busy_r)

    if 'enter' in features_selected:
        # counts the number of instances entering s in the given frame
        enter_s = {('enter', s): 0 for s in S}
        hlf_init.append(enter_s)
    if 'exit' in features_selected:
        # counts the number of instances exiting s in the given frame
        exit_s = {('exit', s): 0 for s in S}
        hlf_init.append(exit_s)
    if 'cross' in features_selected:
        # counts the number of instances residing at s in the given frame
        # either entered during this frame or some frame before
        cross_s = {('cross', s): 0 for s in S}
        hlf_init.append(cross_s)
    if 'wait' in features_selected:
        # measures the average waiting time for the instances residing at s in the given frame
        # needs to be divided by # steps crossing in the end
        wait_s = {('wait', s): 0 for s in S}
        hlf_init.append(wait_s)

    for frame in eval_init.keys():
        for f_init in hlf_init:
            eval_init[frame].update(f_init)

    return eval_init, A, R, S


def eval_hlf(activity_set, resource_set, segment_set, event_dic, trig_dic, window_borders_dict, id_window_mapping,
             id_pairs, res_info, act_selected, res_selected, features_selected):

    frames = sorted(window_borders_dict.keys())
    eval_hlf_complete, A, R, S = init_eval_hlf(activity_set, resource_set, segment_set, frames, act_selected,
                                               res_selected, features_selected)

    # the events that occur last in their trace
    last_events = [i for i in event_dic.keys() if event_dic[i]['single'] or len(trig_dic[i]) == 0]
    for event_id in last_events:
        w = id_window_mapping[event_id]
        ai = event_dic[event_id]['act']
        if ai in A:
            if 'dequeue' in features_selected:
                eval_hlf_complete[w][('dequeue', ai)] += 1
            if 'queue' in features_selected:
                eval_hlf_complete[w][('queue', ai)] += 1
            # enqueue_s is left out as it remains 0 (no upcoming event)

        if res_info:
            ri = event_dic[event_id]['res']
            if ri in R:
                if 'do' in features_selected:
                    eval_hlf_complete[w][('do', ri)] += 1
                if 'busy' in features_selected:
                    eval_hlf_complete[w][('busy', ri)] += 1
                # todo_r is left out as it remains 0 (no upcoming event)

    # going through all steps
    for i, j in id_pairs:
        w_i = id_window_mapping[i]
        w_j = id_window_mapping[j]

        ai, aj = event_dic[i]['act'], event_dic[j]['act']
        ts_i, ts_j = event_dic[i]['ts'], event_dic[j]['ts']
        s = (ai, aj)

        if ai in A:
            if 'dequeue' in features_selected:
                eval_hlf_complete[w_i][('dequeue', ai)] += 1
            if 'queue' in features_selected:
                eval_hlf_complete[w_i][('queue', ai)] += 1

        if s in S:
            if 'enter' in features_selected:
                eval_hlf_complete[w_i][('enter', s)] += 1
            if 'exit' in features_selected:
                eval_hlf_complete[w_j][('exit', s)] += 1
            if 'cross' in features_selected:
                eval_hlf_complete[w_j][('cross', s)] += 1
            if 'wt' in features_selected:
                eval_hlf_complete[w_j][('wt', s)] += ts_j - ts_i

        if res_info:
            ri = event_dic[i]['res']
            if ri in R:
                if 'do' in features_selected:
                    eval_hlf_complete[w_i][('do', ri)] += 1
                if 'busy' in features_selected:
                    eval_hlf_complete[w_i][('busy', ri)] += 1

        for w in range(w_i, w_j):  # w_i included, w_j not included
            if aj in A and 'enqueue' in features_selected:
                eval_hlf_complete[w][('enqueue', aj)] += 1
            if s in S:
                if 'cross' in features_selected:
                    eval_hlf_complete[w][('cross', s)] += 1
                if 'wt' in features_selected:
                    w_right_border = window_borders_dict[w][1]
                    eval_hlf_complete[w][('wt', s)] += w_right_border - ts_i
            if res_info:
                rj = event_dic[j]['res']
                if rj in R:
                    if 'todo' in features_selected:
                        eval_hlf_complete[w][('todo', rj)] += 1
                    if 'busy' in features_selected:
                        eval_hlf_complete[w][('busy', rj)] += 1

    # for frame in eval_hlf_complete.keys():
    #     for s in S:
    #         progr = eval_hlf_complete[frame][('cross', s)]
    #         if progr > 0:
    #             eval_hlf_complete[frame][('wt', s)] /= progr

    return eval_hlf_complete


def eval_hlf_selection_window(eval_complete_w, selected_f_list):
    eval_selected_w = {}
    for f, comp in eval_complete_w.keys():
        if f in selected_f_list:
            eval_selected_w[(f, comp)] = eval_complete_w[(f, comp)]

    return eval_selected_w


def eval_hlf_selection(cs_complete, selected_f_list):
    eval_selected = {}
    for frame in cs_complete.keys():
        eval_complete_w = cs_complete[frame]
        eval_filtered_w = eval_hlf_selection_window(eval_complete_w, selected_f_list)
        eval_selected[frame] = eval_filtered_w

    return eval_selected
