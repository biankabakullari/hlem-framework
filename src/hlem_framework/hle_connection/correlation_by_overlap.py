import networkx as nx
from hle_connection.overlap import case_overlap, place_overlap


def not_candidate_pairs(spread_dic):

    not_candidate_pairs_list = []

    ids_sorted_start_by_first = sorted(spread_dic.keys(), key=lambda x: spread_dic[x]['start-spread-first'])
    ids_windows_start_by_first = [(hle_id, spread_dic[hle_id]['start-spread-first']) for hle_id in
                                  ids_sorted_start_by_first]

    ids_sorted_start_by_last = sorted(spread_dic.keys(), key=lambda x: spread_dic[x]['start-spread-last'])
    ids_windows_start_by_last = [(hle_id, spread_dic[hle_id]['start-spread-last']) for hle_id in
                                 ids_sorted_start_by_last]

    ids_sorted_end_by_first = sorted(spread_dic.keys(), key=lambda x: spread_dic[x]['end-spread-first'])
    ids_windows_end_by_first = [(hle_id, spread_dic[hle_id]['end-spread-first']) for hle_id in
                                ids_sorted_end_by_first]

    ids_sorted_end_by_last = sorted(spread_dic.keys(), key=lambda x: spread_dic[x]['end-spread-last'])
    ids_windows_end_by_last = [(hle_id, spread_dic[hle_id]['end-spread-last']) for hle_id in
                               ids_sorted_end_by_last]

    current_end_first = 0
    # (hle1, hle2) not a candidate if the last window of the start spread of hle2 is before the first window of the end
    # spread of hle1
    for hle_id_2, w_start_last_2 in ids_windows_start_by_last:
        length_end_by_first = len(ids_windows_end_by_first)
        while current_end_first < length_end_by_first and w_start_last_2 < ids_windows_end_by_first[current_end_first][1]:
            hle_id_1 = ids_windows_end_by_first[current_end_first][0]
            not_candidate_pairs_list.append((hle_id_1, hle_id_2))
            current_end_first += 1

    current_start_first = 0
    # (hle1, hle2) not a candidate if the last window of the end spread of hle1 is before the first window of the start
    # spread of hle2
    for hle_id_1, w_end_last_1 in ids_windows_end_by_last:
        length_start_by_first = len(ids_windows_start_by_first)
        while current_start_first < length_start_by_first and w_end_last_1 < ids_windows_start_by_first[current_start_first][1]:
            hle_id_2 = ids_windows_start_by_first[current_start_first][0]
            not_candidate_pairs_list.append((hle_id_1, hle_id_2))
            current_start_first += 1

    return set(not_candidate_pairs_list)


def connected_pairs(hle_all_dic, spread_dict, case_set_dic, not_candidates, co_thresh):

    connected_pairs_list = []

    for hle_id_1 in hle_all_dic.keys():
        for hle_id_2 in hle_all_dic.keys():
            if hle_id_1 != hle_id_2 and (hle_id_1, hle_id_2) not in not_candidates:
                pl_overlap = place_overlap(hle_all_dic, hle_id_1, hle_id_2)
                if pl_overlap:
                    hle1_end = set([w for w in range(spread_dict[hle_id_1]['end-spread-first'],
                                                     spread_dict[hle_id_1]['end-spread-last']+1)])
                    hle2_start = set([w for w in range(spread_dict[hle_id_2]['start-spread-first'],
                                                       spread_dict[hle_id_2]['start-spread-last']+1)])
                    # use time intersection non-empty criterion
                    # intersection_1 = hle1_end.intersection(hle2_start)
                    # intersection_2 = hle2_start.intersection(hle1_end)
                    # if len(intersection_1) > 0 or len(intersection_2) > 0:
                    if hle1_end.issubset(hle2_start) or hle2_start.issubset(hle1_end):
                        case_overlap_ratio = case_overlap(hle_id_1, hle_id_2, case_set_dic)[1]
                        if case_overlap_ratio >= co_thresh:
                            connected_pairs_list.append((hle_id_1, hle_id_2))

    return connected_pairs_list


def hle_graph(hle_all_dic, spread_dic, case_set_dic, co_thresh):

    not_candidates = not_candidate_pairs(spread_dic)
    connected_pairs_list = connected_pairs(hle_all_dic, spread_dic, case_set_dic, not_candidates, co_thresh)

    G = nx.DiGraph()

    nodes = hle_all_dic.keys()
    arcs = connected_pairs_list

    G.add_nodes_from(nodes)
    G.add_edges_from(arcs)

    return G


def cascade_id(graph):
    undirected_graph = graph.to_undirected()
    cascades_dict = {}
    cc = nx.connected_components(undirected_graph)
    no_cascades = 0
    for i, C in enumerate(cc):
        for node in C:
            cascades_dict[node] = i
            no_cascades = i
    print('There are ' + str(len(graph.nodes())) + ' high-level events.')
    print('There are ' + str(no_cascades) + ' connected components/cascades')
    return cascades_dict
