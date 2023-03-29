def evaluation(instance_hlf_w_complete, instance_hlf_w_pair_complete, event_dic, id_window_mapping, window_borders_dic):

    windows = instance_hlf_w_complete.keys()
    eval_window = {w: {} for w in windows}

    for w in windows:
        hlf_as_keys = instance_hlf_w_complete[w].keys()
        for hlf in hlf_as_keys:
            instances = instance_hlf_w_complete[w][hlf]

            if len(instances) > 0:
                f_type = hlf[0]
                if f_type == 'handover':
                    res_group_1 = set([event_dic[i]['res'] for (i, j) in instances])
                    res_group_2 = set([event_dic[j]['res'] for (i, j) in instances])
                    val = len(res_group_1) / len(res_group_2)

                elif f_type == 'workload':
                    no_tasks = len(instances)
                    resources = set([event_dic[i]['res'] for (i, j) in instances])
                    val = no_tasks / len(resources)

                elif f_type == 'wt':
                    waiting_times = []
                    for id_pair in instances:
                        first_id = id_pair[0]
                        first_ts = event_dic[first_id]['ts-seconds']
                        second_id = id_pair[1]
                        second_event_window = id_window_mapping[second_id]
                        if second_event_window == w:  # second_event_window = w
                            second_ts = event_dic[second_id]['ts-seconds']
                            waiting_times.append(second_ts-first_ts)
                        else:  # second_event_window > w
                            w_right_border = window_borders_dic[w][1]
                            waiting_times.append(w_right_border - first_ts)
                    val = sum(waiting_times) / len(instances)
                else:  # f-type not handover, workload or wt
                    val = len(instances)  # for f-type exec, to-exec, do, tod, busy
            else:  # no instances
                val = 0

            eval_window[w][hlf] = val

    # same procedure for window pairs
    window_pairs = instance_hlf_w_pair_complete.keys()
    eval_window_pairs = {w_pair: dict() for w_pair in window_pairs}
    for w_pair in window_pairs:
        w_pair_keys = instance_hlf_w_pair_complete[w_pair].keys()
        for hlf in w_pair_keys:
            f_type = hlf[0]
            instances = instance_hlf_w_pair_complete[w_pair][hlf]
            no_instances = len(instances)
            if f_type == 'batch':
                val = no_instances
            elif f_type == 'delay':
                val = (w_pair[1] - w_pair[0], no_instances)  # (no windows in between, no_instances)
            else:
                val = 0
            eval_window_pairs[w_pair][hlf] = val

    eval_theta = eval_window.copy()
    eval_theta.update(eval_window_pairs)
    return eval_theta
