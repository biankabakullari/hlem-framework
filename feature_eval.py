def init_eval_hlf(activity_set, resource_set, segment_set, frames, act_selected, res_selected):
    eval_init = {frame: {} for frame in frames}

    if act_selected == 'all':
        A = activity_set
        S = segment_set
    else:
        A = act_selected
        S = [(s1, s2) for (s1, s2) in segment_set if s1 in A and s2 in A]
    exec_a = {('exec', a): 0 for a in A}
    exec_ld_a = {('exec-ld', a): 0 for a in A}

    if res_selected == 'all':
        R = resource_set
    else:
        R = res_selected
    do_r = {('do', r): 0 for r in R}
    todo_r = {('todo', r): 0 for r in R}
    wl_r = {('wl', r): 0 for r in R}

    enter_s = {('enter', s): 0 for s in S}
    exit_s = {('exit', s): 0 for s in S}
    progr_s = {('progr', s): 0 for s in S}
    wt_s = {('wt', s): 0 for s in S}  # needs to be normalized in the end
    hlf_init = [exec_a, do_r, todo_r, wl_r, enter_s, exit_s, progr_s, wt_s]
    for frame in eval_init.keys():
        for f_init in hlf_init:
            eval_init[frame].update(f_init)

    return eval_init, A, R, S


def eval_hlf(activity_set, resource_set, segment_set, event_dic, trig_dic, window_borders_dict, id_window_mapping,
             id_pairs, res_info, act_selected, res_selected):

    frames = sorted(window_borders_dict.keys())
    eval_hlf_complete, A, R, S = init_eval_hlf(activity_set, resource_set, segment_set, frames, act_selected,
                                               res_selected)

    singles = [i for i in event_dic.keys() if event_dic[i]['single'] or len(trig_dic[i]) == 0]
    for i in singles:
        w_i = id_window_mapping[i]
        ai = event_dic[i]['act']
        if ai in A:
            eval_hlf_complete[w_i][('exec', ai)] += 1
            eval_hlf_complete[w_i][('exec-ld', ai)] += 1
        if res_info:
            ri = event_dic[i]['res']
            if ri in R:
                eval_hlf_complete[w_i][('do', ri)] += 1
                eval_hlf_complete[w_i][('wl', ri)] += 1

    for i, j in id_pairs:
        w_i = id_window_mapping[i]
        w_j = id_window_mapping[j]

        ai, aj = event_dic[i]['act'], event_dic[j]['act']
        ts_i, ts_j = event_dic[i]['ts'], event_dic[j]['ts']
        s = (ai, aj)
        if ai in A:
            eval_hlf_complete[w_i][('exec', ai)] += 1
            eval_hlf_complete[w_i][('exec-ld', ai)] += 1

        if s in S:
            eval_hlf_complete[w_i][('enter', s)] += 1
            eval_hlf_complete[w_j][('exit', s)] += 1

        if res_info:
            ri = event_dic[i]['res']
            if ri in R:
                eval_hlf_complete[w_i][('do', ri)] += 1
                eval_hlf_complete[w_i][('wl', ri)] += 1

        for w in range(w_i, w_j):  # w_j not included
            if aj in A:
                eval_hlf_complete[w][('exec-ld', aj)] += 1
            if s in S:
                eval_hlf_complete[w][('progr', s)] += 1
                w_right_border = window_borders_dict[w][1]
                eval_hlf_complete[w][('wt', s)] += w_right_border - ts_i
            if res_info:
                rj = event_dic[j]['res']
                if rj in R:
                    eval_hlf_complete[w][('wl', rj)] += 1

        if s in S:
            eval_hlf_complete[w_j][('progr', s)] += 1
            eval_hlf_complete[w_j][('wt', s)] += ts_j - ts_i

    for frame in eval_hlf_complete.keys():
        for s in S:
            progr = eval_hlf_complete[frame][('progr', s)]
            if progr > 0:
                eval_hlf_complete[frame][('wt', s)] /= progr

    return eval_hlf_complete


def eval_hlf_selection_window(eval_complete_w, selected_f_list):
    eval_filtered_w = {}
    for f, comp in eval_complete_w.keys():
        if f in selected_f_list:
            eval_filtered_w[(f, comp)] = eval_complete_w[(f, comp)]

    return eval_filtered_w


def eval_hlf_selection(cs_complete, selected_f_list):
    eval_filtered = {}
    for frame in cs_complete.keys():
        eval_complete_w = cs_complete[frame]
        eval_filtered_w = eval_hlf_selection_window(eval_complete_w, selected_f_list)
        eval_filtered[frame] = eval_filtered_w

    return eval_filtered
