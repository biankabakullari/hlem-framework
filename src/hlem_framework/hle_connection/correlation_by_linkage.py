import networkx as nx
from typing import List
from collections import defaultdict
from hle_generation.hle_generation_fw import HLE


def w_generates_hle(window_to_id_to_hle, window):
    """
    :param window_to_id_to_hle: a dict with hle ids as keys and corresponding HLE as values
    :param window: some window id
    :return: True iff there is at least one hle arising at that window
    """
    hle_w = window_to_id_to_hle[window]
    return len(hle_w) > 0


def window_entities(hle_w: List[HLE]):
    """
    :param hle_w: A list of high-level events HLE
    :return: list of corresponding entities (names of activities, resources, or segments) of the high-level events
    """
    entities = [hle.entity for hle in hle_w]
    return entities


def two_windows_edge_weights2(w1, w2, window_to_id_to_hle, link_dict):
    """
    :param w1: window id
    :param w2: window id
    :param window_to_id_to_hle: a dict with hle ids as keys and corresponding HLE as values
    :param link_dict: A dict where e.g., link_dict['ar'][('request', 'Jane')]=link_dict['ar'][('Jane', 'request')]
    reflects the link between activity 'request' and resource 'Jane'
    :return: a dictionary where edge_weights_dict[(w1, hl1), (w2, hle2)] is the link value for pair
    (hle1.entity, hle2.entity).
    This function is only called for consecutive non-empty windows.
    """

    edge_weights_dict = defaultdict(lambda: 0)

    hle_w1 = window_to_id_to_hle[w1]  # all hle of w1 as dict with hle ids as keys
    hle_w2 = window_to_id_to_hle[w2]  # all hle of w2 as dict with hle ids as keys

    for hle_i_id in hle_w1.keys():
        hle_i = hle_w1[hle_i_id]
        u = (w1, hle_i_id)  # u=(window id, hle id)
        for hle_j_id in hle_w2.keys():
            hle_j = hle_w2[hle_j_id]
            v = (w2, hle_j_id)  # v=(window id, hle id)
            entity_i, entity_j = hle_i.entity, hle_j.entity
            if entity_i == entity_j:
                uv_weight = 1
            else:
                comp_i, comp_j = hle_i.component, hle_j.component
                key = comp_i[0] + comp_j[0]
                if key == 'ra':
                    key = 'ar'
                elif key == 'sa':
                    key = 'as'
                elif key == 'sr':
                    key = 'rs'
                uv_weight = link_dict[key][(entity_i, entity_j)]
            edge_weights_dict[(u, v)] = uv_weight

    return edge_weights_dict


def hle_graph_weighted(window_to_id_to_hle, link_dict, link_thresh):
    """
    :param window_to_id_to_hle: a dict with hle ids as keys and corresponding HLE as values
    :param link_dict: A dict where e.g., link_dict['ar'][('request', 'Jane')]=link_dict['ar'][('Jane', 'request')]
    reflects the link between activity 'request' and resource 'Jane'
    :param link_thresh: the threshold used for the link value
    :return:
    An undirected graph G where each node is a (w, hle_w) where w a window and hle_w a HLE captured inside w, and edges
    between (w1,hle1) and (w2,hle2) whenever the link between hle1.entity and hle2.entity is above the link_thresh
    """

    windows = sorted([window for window in window_to_id_to_hle.keys()])
    G = nx.Graph()

    last_opened = False
    for i, this_window in enumerate(windows):
        hle_w = window_to_id_to_hle[this_window]
        if len(hle_w) > 0:  # w non-empty, will become last window
            v_nodes = [(this_window, i) for i in hle_w.keys()]
            G.add_nodes_from(v_nodes)
            if last_opened:  # the previous window has high-level events
                previous_window = windows[i - 1]
                edge_weights_previous_now = two_windows_edge_weights2(previous_window, this_window, window_to_id_to_hle, link_dict)
                for u, v in edge_weights_previous_now.keys():
                    uv_weight = edge_weights_previous_now[(u, v)]
                    if uv_weight > link_thresh:
                        G.add_edge(u, v)
            last_opened = True

        else:  # w is empty
            last_opened = False

    return G


def cascade_id(graph):
    """
    :param graph: An undirected graph
    :return: a dictionary assigning IDs to graph's vertices, such that two vertices get the same ID iff they are in the
    same connected component
    """
    cascades_dict = {}
    cc = nx.connected_components(graph)

    for i, C in enumerate(cc):
        for node in C:
            cascades_dict[node] = i

    return cascades_dict
