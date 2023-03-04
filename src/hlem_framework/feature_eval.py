def init_eval_hlf(activity_set, resource_set, segment_set, frames, act_selected, res_selected, features_selected):

    eval_init = {frame: {} for frame in frames}
    hlf_init = []

    if act_selected == 'all':
        A = activity_set
        S = segment_set
    else:
        A = act_selected
        S = [(s1, s2) for (s1, s2) in segment_set if s1 in A and s2 in A]

    if 'exec' in features_selected:
        exec_a = {('exec', a): 0 for a in A}
        hlf_init.append(exec_a)
    if 'to-exec' in features_selected:
        to_exec_a = {('to-exec', a): 0 for a in A}
        hlf_init.append(to_exec_a)

    if res_selected == 'all':
        R = resource_set
    else:
        R = res_selected
    if len(R) > 0:
        if 'do' in features_selected:
            do_r = {('do', r): 0 for r in R}
            hlf_init.append(do_r)
        if 'todo' in features_selected:
            todo_r = {('todo', r): 0 for r in R}
            hlf_init.append(todo_r)
        if 'busy' in features_selected:
            busy_r = {('busy', r): 0 for r in R}
            hlf_init.append(busy_r)

    if 'enter' in features_selected:
        enter_s = {('enter', s): 0 for s in S}
        hlf_init.append(enter_s)
    if 'exit' in features_selected:
        exit_s = {('exit', s): 0 for s in S}
        hlf_init.append(exit_s)
    if 'cross' in features_selected:
        cross_s = {('cross', s): 0 for s in S}
        hlf_init.append(cross_s)
    if 'wt' in features_selected:
        wt_s = {('wt', s): 0 for s in S}  # needs to be divided by # cases in progress in the end
        hlf_init.append(wt_s)

    for frame in eval_init.keys():
        for f_init in hlf_init:
            eval_init[frame].update(f_init)

    return eval_init, A, R, S


def eval_hlf(activity_set, resource_set, segment_set, event_dic, trig_dic, window_borders_dict, id_window_mapping,
             id_pairs, res_info, act_selected, res_selected, features_selected):

    frames = sorted(window_borders_dict.keys())
    eval_hlf_complete, A, R, S = init_eval_hlf(activity_set, resource_set, segment_set, frames, act_selected,
                                               res_selected, features_selected)

    singles = [i for i in event_dic.keys() if event_dic[i]['single'] or len(trig_dic[i]) == 0]

    for i in singles:
        w_i = id_window_mapping[i]
        ai = event_dic[i]['act']
        if ai in A:
            if 'exec' in features_selected:
                eval_hlf_complete[w_i][('exec', ai)] += 1
            if 'to-exec' in features_selected:
                eval_hlf_complete[w_i][('to-exec', ai)] += 1

        if res_info:
            ri = event_dic[i]['res']
            if ri in R:
                if 'do' in features_selected:
                    eval_hlf_complete[w_i][('do', ri)] += 1
                if 'busy' in features_selected:
                    eval_hlf_complete[w_i][('busy', ri)] += 1
                # todor is left out as it remains 0

    for i, j in id_pairs:
        w_i = id_window_mapping[i]
        w_j = id_window_mapping[j]

        ai, aj = event_dic[i]['act'], event_dic[j]['act']
        ts_i, ts_j = event_dic[i]['ts'], event_dic[j]['ts']
        s = (ai, aj)

        if ai in A:
            if 'exec' in features_selected:
                eval_hlf_complete[w_i][('exec', ai)] += 1
            if 'to-exec' in features_selected:
                eval_hlf_complete[w_i][('to-exec', ai)] += 1

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
            if aj in A and 'to-exec' in features_selected:
                eval_hlf_complete[w][('to-exec', aj)] += 1
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
