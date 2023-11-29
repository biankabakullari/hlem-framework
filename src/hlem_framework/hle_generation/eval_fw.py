class PathsAndCasesArgs:
    frame: FrameType = 'days'
    traffic_type: str = 'High'
    act_selection: ActSelection = 'all'
    seg_percentile: Union[float, int]
    selected_f_list = DEFAULT_HLF
    p: Union[float, int] = 0.9
    co_thresh: Union[float, int] = 0.5
    co_path_thresh: Union[float, int] = 0.5
    res_info: bool = True
    only_maximal_paths: bool = True
    path_frequency = 0
    res_selection = 'all'
    seg_method = 'df'
    type_based = True
    seg_percentile = 0.8

    def validate_percentile(self):
        if not (0 <= self.seg_percentile <= 1):
            raise ValueError('seg_percentile must be in range 0 - 1')

    def validate_res(self):
        if not self.res_info and self.res_selection:
            raise ValueError('put res_info to True if you want to analyze resources')

    def validate(self):
        self.validate_percentile()
        self.validate_res()


def init_eval_hlf(frames, A_focus, S_focus, R_focus, features):
    """
    :param frames: a list of numbers 0,1,2,..., each uniquely identifying a window
    :param A_focus: the set of activities chosen for analysis
    :param S_focus: the set of segments chosen for analysis
    :param R_focus: the set of resources chosen for analysis
    :param features: the set of selected features for analysis (e.g., 'enter', 'delay', 'wl', ...)
    :return:
    -   eval_init: a dict with first level key value pairs: window id, dictionary for that window, and second level key
    value pairs: {(enqueue,a):0, (enqueue,b):0,...}, {(enter,(a,b)):0, (enter,(c,d)):0,...}, {(busy,r1):0, (busy,r2)...}
    """
    eval_init = {frame: {} for frame in frames}
    hlf_init = []

    if 'exec' in features:
        # counts the instances executing a at a given frame
        exec_a = {('exec', a): 0 for a in A_focus}
        hlf_init.append(exec_a)
    if 'toexec' in features:
        # counts the instances enqueuing for a at a given frame (the last previous event happened at frame)
        toexec_a = {('toexec', a): 0 for a in A_focus}
        hlf_init.append(toexec_a)
    if 'queue' in features:
        # counts the instances in queue waiting for a at the given frame
        # either enqueued in this frame, or some frame before
        queue_a = {('queue', a): 0 for a in A_focus}
        hlf_init.append(queue_a)

    if len(R_focus) > 0:
        if 'do' in features:
            # counts the number of events resource r executes within the given frame
            do_r = {('do', r): 0 for r in R_focus}
            hlf_init.append(do_r)
        if 'todo' in features:
            # counts the number of events added to the task list of resource r executes within the given frame
            # (the last previous event happened at frame)
            todo_r = {('todo', r): 0 for r in R_focus}
            hlf_init.append(todo_r)
        if 'busy' in features:
            # counts the number of events in the task list of resource r at the given frame
            # either added in this frame, or some frame before
            busy_r = {('busy', r): 0 for r in R_focus}
            hlf_init.append(busy_r)

    if 'enter' in features:
        # counts the number of instances entering s in the given frame
        enter_s = {('enter', s): 0 for s in S_focus}
        hlf_init.append(enter_s)
    if 'exit' in features:
        # counts the number of instances exiting s in the given frame
        exit_s = {('exit', s): 0 for s in S_focus}
        hlf_init.append(exit_s)

    if 'wait' in features:
        # measures the average waiting time for the instances residing at s in the given frame
        # needs to be divided by # steps crossing in the end
        wait_s = {('wait', s): 0 for s in S_focus}
        hlf_init.append(wait_s)

        # counts the number of instances residing at s in the given frame
        # either entered during this frame or some frame before
        # if wait is selected, you need cross to compute the average waiting time
        cross_s = {('cross', s): 0 for s in S_focus}
        hlf_init.append(cross_s)
    else:  # wait not in selected features
        # add cross only if it is explicitly part of the selected features
        if 'cross' in features:
            cross_s = {('cross', s): 0 for s in S_focus}
            hlf_init.append(cross_s)

    for frame in eval_init.keys():
        for f_init in hlf_init:
            eval_init[frame].update(f_init)

    return eval_init


def eval_hlf(event_dic, trig_dic, window_to_borders, ev_to_window, id_pairs, res_info, A_focus, S_focus, R_focus, features):

    frames = sorted(window_to_borders.keys())
    eval_hlf_complete = init_eval_hlf(frames, A_focus, S_focus, R_focus, features)

    # the events that occur last in their trace
    last_events = [i for i in event_dic.keys() if event_dic[i]['single'] or len(trig_dic[i]) == 0]
    for event_id in last_events:
        w = ev_to_window[event_id]
        ai = event_dic[event_id]['act']
        if ai in A_focus:
            if 'exec' in features:
                eval_hlf_complete[w][('exec', ai)] += 1
            if 'queue' in features:
                eval_hlf_complete[w][('queue', ai)] += 1
            # toexec_x is left out as it remains 0 (no upcoming event)

        if res_info:
            ri = event_dic[event_id]['res']
            if ri in R_focus:
                if 'do' in features:
                    eval_hlf_complete[w][('do', ri)] += 1
                if 'busy' in features:
                    eval_hlf_complete[w][('busy', ri)] += 1
                # todo_x is left out as it remains 0 (no upcoming event)

    # going through all steps
    for i, j in id_pairs:
        w_i = ev_to_window[i]
        w_j = ev_to_window[j]

        ai, aj = event_dic[i]['act'], event_dic[j]['act']
        ts_i, ts_j = event_dic[i]['ts'], event_dic[j]['ts']
        s = (ai, aj)

        if ai in A_focus:
            if 'exec' in features:
                eval_hlf_complete[w_i][('exec', ai)] += 1
            if 'queue' in features:
                eval_hlf_complete[w_i][('queue', ai)] += 1

        if s in S_focus:
            if 'enter' in features:
                eval_hlf_complete[w_i][('enter', s)] += 1
            if 'exit' in features:
                eval_hlf_complete[w_j][('exit', s)] += 1
            if 'wait' in features:
                eval_hlf_complete[w_j][('wait', s)] += ts_j - ts_i
                eval_hlf_complete[w_j][('cross', s)] += 1
            else:
                if 'cross' in features:
                    eval_hlf_complete[w_j][('cross', s)] += 1

        if res_info:
            ri = event_dic[i]['res']
            if ri in R_focus:
                if 'do' in features:
                    eval_hlf_complete[w_i][('do', ri)] += 1
                if 'busy' in features:
                    eval_hlf_complete[w_i][('busy', ri)] += 1

        for w in range(w_i, w_j):  # w_i included, w_j not included
            if aj in A_focus and 'toexec' in features:
                eval_hlf_complete[w][('toexec', aj)] += 1
            if s in S_focus:
                if 'wait' in features:
                    # if wait is selected, then always also compute cross (needed for average waiting time)
                    w_right_border = window_to_borders[w][1]
                    eval_hlf_complete[w][('wait', s)] += w_right_border - ts_i
                    eval_hlf_complete[w][('cross', s)] += 1
                else:
                    # if wait not selected, compute cross only when explicitly selected as feature
                    if 'cross' in features:
                        eval_hlf_complete[w][('cross', s)] += 1

            if res_info:
                rj = event_dic[j]['res']
                if rj in R_focus:
                    if 'todo' in features:
                        eval_hlf_complete[w][('todo', rj)] += 1
                    if 'busy' in features:
                        eval_hlf_complete[w][('busy', rj)] += 1

        # computing the average waiting time at any segment s at any window w
        # as total accumulated waiting time until w / number of instances crossing s
        if 'wait' in features:
            for frame in eval_hlf_complete.keys():
                for s in S_focus:
                    crossing_count = eval_hlf_complete[frame][('cross', s)]
                    if crossing_count > 0:
                        eval_hlf_complete[frame][('wait', s)] /= crossing_count

    return eval_hlf_complete



#TODO: code below unnecessary if the eval functions above only compute hle for the selected features anyway

# def eval_hlf_selection_window(eval_complete_w, selected_f_list):
#     eval_selected_w = {}
#     for f, comp in eval_complete_w.keys():
#         if f in selected_f_list:
#             eval_selected_w[(f, comp)] = eval_complete_w[(f, comp)]
#
#     return eval_selected_w
#
#
# def eval_hlf_selection(cs_complete, selected_f_list):
#     eval_selected = {}
#     for frame in cs_complete.keys():
#         eval_complete_w = cs_complete[frame]
#         eval_filtered_w = eval_hlf_selection_window(eval_complete_w, selected_f_list)
#         eval_selected[frame] = eval_filtered_w
#
#     return eval_selected
