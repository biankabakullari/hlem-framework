import networkx as nx
import math
from collections import defaultdict


# a window generates a hle iff it has at least one hle
def w_generates_hle(hle_all, window):
    hle_w = hle_all[window]
    return len(hle_w) > 0


def window_entities(hle_w):
    comps = [hle[1] for hle in hle_w]
    return comps


# call only if both windows have high-level events
def two_windows_edge_weights(w1, w2, hle_w1, hle_w2, link_dic, comp_type_dic):

    edge_weights_dict = defaultdict(lambda: 0)
    entities_w1, entities_w2 = window_entities(hle_w1), window_entities(hle_w2)

    for i, hle_i in enumerate(hle_w1):
        u = (w1, i)
        for j, hle_j in enumerate(hle_w2):
            v = (w2, j)
            entity_i, entity_j = entities_w1[i], entities_w2[j]
            if entity_i == entity_j:
                uv_weight = 1
            else:
                type_i, type_j = comp_type_dic[entity_i], comp_type_dic[entity_j]
                key = type_i[0] + type_j[0]
                if key == 'ra':
                    key = 'ar'
                elif key == 'sa':
                    key = 'as'
                elif key == 'sr':
                    key = 'rs'

                uv_weight = link_dic[key][(entity_i, entity_j)]
            edge_weights_dict[(u, v)] = uv_weight

    return edge_weights_dict


def hle_graph_weighted(hle_all_w, link_dic, comp_type_dic, thresh):

    windows = hle_all_w.keys()
    no_windows = len(windows)
    G = nx.Graph()
    max_scope = min(10, math.ceil(no_windows / 100))

    last_opened = False
    last_hle = []

    for w in windows:
        hle_w = hle_all_w[w]
        w_number_hle = len(hle_w)
        if w_number_hle > 0:  # w non-empty, will become last window
            v_nodes = [(w, i) for i in range(w_number_hle)]
            G.add_nodes_from(v_nodes)
            if last_opened:
                edge_weights_last_now = two_windows_edge_weights(w-1, w, last_hle, hle_w, link_dic, comp_type_dic)
                for u, v in edge_weights_last_now.keys():
                    cc_u_size = len(nx.node_connected_component(G, u))
                    uv_weight = edge_weights_last_now[(u, v)]
                    if cc_u_size < max_scope:
                        if uv_weight >= thresh or uv_weight == 1:
                            G.add_edge(u, v)
            last_opened = True
            last_hle = hle_w

        else:  # w is empty
            last_opened = False
            last_hle = []

    return G


def cascade_id(graph):
    cascades_dict = {}
    cc = nx.connected_components(graph)

    for i, C in enumerate(cc):
        for node in C:
            cascades_dict[node] = i

    return cascades_dict
