import networkx as nx
from collections import defaultdict


# a window generates a hle iff it has at least one hle
def w_generates_hle(hle_all_by_theta, window):
    hle_w = hle_all_by_theta[window]
    return len(hle_w) > 0


def window_entities(hle_w):
    entities = [hle['entity'] for hle in hle_w]
    return entities


# call only if both windows have high-level events
# def two_windows_edge_weights(w1, w2, hle_w1, hle_w2, link_dic, comp_type_dic):
#
#     edge_weights_dict = defaultdict(lambda: 0)
#     entities_w1, entities_w2 = window_entities(hle_w1), window_entities(hle_w2)
#
#     for i, hle_i in enumerate(hle_w1):
#         u = (w1, i)
#         for j, hle_j in enumerate(hle_w2):
#             v = (w2, j)
#             entity_i, entity_j = entities_w1[i], entities_w2[j]
#             if entity_i == entity_j:
#                 uv_weight = 1
#             else:
#                 type_i, type_j = comp_type_dic[entity_i], comp_type_dic[entity_j]
#                 key = type_i[0] + type_j[0]
#                 if key == 'ra':
#                     key = 'ar'
#                     uv_weight = link_dic[key][(entity_j, entity_i)]
#                 elif key == 'sa':
#                     key = 'as'
#                     uv_weight = link_dic[key][(entity_j, entity_i)]
#                 elif key == 'sr':
#                     key = 'rs'
#                     uv_weight = link_dic[key][(entity_j, entity_i)]
#
#                 else:
#                     uv_weight = link_dic[key][(entity_i, entity_j)]
#             edge_weights_dict[(u, v)] = uv_weight
#
#     return edge_weights_dict


# call only if both windows have high-level events
def two_windows_edge_weights2(w1, w2, hle_all_by_theta, link_dic):

    edge_weights_dict = defaultdict(lambda: 0)

    hle_w1 = hle_all_by_theta[w1]  # all hle of w1 with inner hle ids as keys
    hle_w2 = hle_all_by_theta[w2]  # all hle of w2 with inner hle ids as keys

    for hle_i_id in hle_w1.keys():
        hle_i = hle_w1[hle_i_id]
        u = (w1, hle_i_id)  # (window id, hle id)
        for hle_j_id in hle_w2.keys():
            hle_j = hle_w2[hle_j_id]
            v = (w2, hle_j_id)  # (window id, hle id)
            entity_i, entity_j = hle_i['entity'], hle_j['entity']
            if entity_i == entity_j:
                uv_weight = 1
            else:
                comp_i, comp_j = hle_i['component'], hle_j['component']
                key = comp_i[0] + comp_j[0]
                if key == 'ra':
                    key = 'ar'
                    uv_weight = link_dic[key][(entity_j, entity_i)]
                elif key == 'sa':
                    key = 'as'
                    uv_weight = link_dic[key][(entity_j, entity_i)]
                elif key == 'sr':
                    key = 'rs'
                    uv_weight = link_dic[key][(entity_j, entity_i)]

                else:
                    uv_weight = link_dic[key][(entity_i, entity_j)]
            edge_weights_dict[(u, v)] = uv_weight

    return edge_weights_dict


def hle_graph_weighted(hle_all_by_theta, link_dic, thresh):

    windows = sorted([theta for theta in hle_all_by_theta.keys() if isinstance(theta, int)])  # only single windows
    # window_pairs = [theta for theta in hle_all_by_theta.keys() if not isinstance(theta, int)]
    # print(window_pairs)
    G = nx.Graph()

    # no_windows = len(windows)
    # max_scope = min(10, math.ceil(no_windows / 100))

    last_opened = False
    for i, w in enumerate(windows):
        hle_w = hle_all_by_theta[w]
        w_number_hle = len(hle_w)
        if w_number_hle > 0:  # w non-empty, will become last window
            v_nodes = [(w, i) for i in hle_w.keys()]
            G.add_nodes_from(v_nodes)
            if last_opened:  # the previous window has high-level events
                last_window = windows[i - 1]
                this_window = windows[i]
                edge_weights_last_now = two_windows_edge_weights2(last_window, this_window, hle_all_by_theta, link_dic)
                for u, v in edge_weights_last_now.keys():
                    uv_weight = edge_weights_last_now[(u, v)]
                    # cc_u_size = len(nx.node_connected_component(G, u))
                    # if cc_u_size < max_scope:
                    if uv_weight >= thresh:
                        G.add_edge(u, v)
            last_opened = True

        else:  # w is empty
            last_opened = False

    return G


def cascade_id(graph):
    cascades_dict = {}
    cc = nx.connected_components(graph)

    for i, C in enumerate(cc):
        for node in C:
            cascades_dict[node] = i

    return cascades_dict
