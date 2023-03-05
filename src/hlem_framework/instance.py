from copy import deepcopy
from tqdm import tqdm


def init_instances(activity_set, resource_set, segment_set, frames, act_selected, res_selected, features_selected):

    instance_init_w = {frame: dict() for frame in frames}
    if 'batch' in features_selected or 'delay' in features_selected:
        frame_pairs = [(w1, w2) for w1 in frames for w2 in frames if w1 < w2]  # maybe change to <=
    else:
        frame_pairs = []
    instance_init_w_pair = {w_pair: dict() for w_pair in frame_pairs}

    hlf_init_w = []  # list of dictionaries, one dic per each f-type with (f-type,entity) as keys
    hlf_init_w_pair = []  # list of dictionaries

    if act_selected == 'all':
        A = activity_set
        S = segment_set
    else:
        A = act_selected
        S = [(s1, s2) for (s1, s2) in segment_set if s1 in A and s2 in A]

    if 'exec' in features_selected:
        exec_a = {('exec', a): [] for a in A}
        hlf_init_w.append(exec_a)
    if 'to-exec' in features_selected:
        to_exec_a = {('to-exec', a): [0] for a in A}
        hlf_init_w.append(to_exec_a)

    if res_selected == 'all':
        R = resource_set
    else:
        R = res_selected
    if len(R) > 0:
        if 'do' in features_selected:
            do_r = {('do', r): [] for r in R}
            hlf_init_w.append(do_r)
        if 'todo' in features_selected:
            todo_r = {('todo', r): [] for r in R}
            hlf_init_w.append(todo_r)
        if 'busy' in features_selected:
            busy_r = {('busy', r): [] for r in R}
            hlf_init_w.append(busy_r)

    if 'enter' in features_selected:
        enter_s = {('enter', s): [] for s in S}
        hlf_init_w.append(enter_s)
    if 'exit' in features_selected:
        exit_s = {('exit', s): [] for s in S}
        hlf_init_w.append(exit_s)
    if 'cross' in features_selected:
        cross_s = {('cross', s): [] for s in S}
        hlf_init_w.append(cross_s)
    if 'wt' in features_selected:
        wt_s = {('wt', s): [] for s in S}
        hlf_init_w.append(wt_s)
    if 'handover' in features_selected:
        handover_s = {('handover', s): [] for s in S}
        hlf_init_w.append(handover_s)
    if 'workload' in features_selected:
        workload_s = {('workload', s): [] for s in S}
        hlf_init_w.append(workload_s)

    if 'batch' in features_selected:
        batch_s = {('batch', s): [] for s in S}
        hlf_init_w_pair.append(batch_s)
    if 'delay' in features_selected:
        delay_s = {('delay', s): [] for s in S}
        hlf_init_w_pair.append(delay_s)

    # all individual frames are initiated with empty instance list for the hlf which are window-based
    for frame in instance_init_w.keys():  # for each window
        for hlf_w_dic in hlf_init_w:  # for each f-type dic from the list of dictionaries
            # DELETE? if frame in hlf_w_dic.keys():
            instance_init_w[frame].update(deepcopy(hlf_w_dic))  # extend dictionary at the frame with each f-type dic of the frame

    # all frame pairs are initiated with empty instance list for the hlf which are window pair-based
    if len(frame_pairs) > 0:
        for frame_pair in instance_init_w_pair.keys():  # for each window pair
            for hlf_w_pair_dic in hlf_init_w_pair:  # for each f-type dic from the list of dictionaries
                # DELETE? if frame_pair in hlf_w_pair_dic.keys():
                # extend dictionary at the window pair with each f-type dic of the window pair
                instance_init_w_pair[frame_pair].update(deepcopy(hlf_w_pair_dic))

    return instance_init_w, instance_init_w_pair, A, R, S


def instances_hlf(activity_set, resource_set, segment_set, event_dic, trig_dic, window_borders_dict, id_window_mapping,
                  id_pairs, res_info, act_selected, res_selected, features_selected):

    single_windows = sorted(window_borders_dict.keys())
    instance_hlf_w_complete, instance_hlf_w_pair_complete, A, R, S = init_instances(activity_set, resource_set,
                                                                                    segment_set, single_windows,
                                                                                    act_selected, res_selected,
                                                                                    features_selected)

    singles = [i for i in event_dic.keys() if event_dic[i]['single'] or len(trig_dic[i]) == 0]

    if len({'exec', 'to-exec', 'do', 'busy'}.intersection(set(features_selected))) > 0:
        for i in singles:
            w_i = id_window_mapping[i]
            ai = event_dic[i]['act']
            if ai in A:
                if 'exec' in features_selected:
                    instance_hlf_w_complete[w_i][('exec', ai)].append(i)
                if 'to-exec' in features_selected:
                    instance_hlf_w_complete[w_i][('to-exec', ai)].append(i)

            if res_info and len({'do', 'busy'}.intersection(set(features_selected))) > 0:
                ri = event_dic[i]['res']
                if ri in R:
                    if 'do' in features_selected:
                        instance_hlf_w_complete[w_i][('do', ri)].append(i)
                    if 'busy' in features_selected:
                        instance_hlf_w_complete[w_i][('busy', ri)].append(i)
                    # todor is left out as it remains 0

    for i, j in tqdm(id_pairs, desc='Event pairs as instances'):
        id_pair = (i, j)
        w_i = id_window_mapping[i]
        w_j = id_window_mapping[j]

        ai, aj = event_dic[i]['act'], event_dic[j]['act']
        s = (ai, aj)

        if ai in A:
            if 'exec' in features_selected:
                instance_hlf_w_complete[w_i][('exec', ai)].append(i)
            if 'to-exec' in features_selected:
                instance_hlf_w_complete[w_i][('to-exec', ai)].append(i)

        if s in S:
            if 'enter' in features_selected:
                instance_hlf_w_complete[w_i][('enter', s)].append(id_pair)
            if 'exit' in features_selected:
                instance_hlf_w_complete[w_j][('exit', s)].append(id_pair)
            if 'cross' in features_selected:
                instance_hlf_w_complete[w_j][('cross', s)].append(id_pair)
            if 'wt' in features_selected:
                instance_hlf_w_complete[w_j][('wt', s)].append(id_pair)

            if res_info:
                ri = event_dic[i]['res']
                rj = event_dic[j]['res']
                if 'workload' in features_selected:
                    if ri == rj and ri in R:
                        instance_hlf_w_complete[w_j][('workload', s)].append(id_pair)
                if 'handover' in features_selected:
                    if ri != rj and ri in R and rj in R:
                        instance_hlf_w_complete[w_j][('handover', s)].append(id_pair)

            if 'batch' in features_selected and w_i < w_j:
                instance_hlf_w_pair_complete[(w_i, w_j)][('batch', s)].append(id_pair)
            if 'delay' in features_selected and w_i < w_j:
                instance_hlf_w_pair_complete[(w_i, w_j)][('delay', s)].append(id_pair)

        if res_info and len({'do', 'busy'}.intersection(set(features_selected))) > 0:
            ri = event_dic[i]['res']
            if ri in R:
                if 'do' in features_selected:
                    instance_hlf_w_complete[w_i][('do', ri)].append(i)
                if 'busy' in features_selected:
                    instance_hlf_w_complete[w_i][('busy', ri)].append(i)

        if len({'to-exec', 'cross', 'wt', 'todo', 'busy'}.intersection(set(features_selected))) > 0:
            for w in range(w_i, w_j):  # w_i included, w_j not included
                if aj in A and 'to-exec' in features_selected:
                    instance_hlf_w_complete[w][('to-exec', aj)].append(j)
                if s in S:
                    if 'cross' in features_selected:
                        instance_hlf_w_complete[w][('cross', s)].append(id_pair)
                    if 'wt' in features_selected:
                        instance_hlf_w_complete[w][('wt', s)].append(id_pair)
                if res_info:
                    rj = event_dic[j]['res']
                    if rj in R:
                        if 'todo' in features_selected:
                            instance_hlf_w_complete[w][('todo', rj)].append(j)
                        if 'busy' in features_selected:
                            instance_hlf_w_complete[w][('busy', rj)].append(j)

    instance_all_complete = instance_hlf_w_complete.copy()
    instance_all_complete.update(instance_hlf_w_pair_complete)

    return instance_hlf_w_complete, instance_hlf_w_pair_complete, instance_all_complete


# maybe the code below unnecessary

# def eval_hlf_selection_window(eval_complete_w, selected_f_list):
#     eval_selected_w = {}
#     for f_type, comp in eval_complete_w.keys():
#         if f_type in selected_f_list:
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
