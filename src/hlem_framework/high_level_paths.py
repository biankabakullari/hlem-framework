import math
import os
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
import overlap
import trie


def find_neighbours_with_sufficient_overlap(digraph, cases_dic, co_path_thresh, from_node, cases_until_node, visited):
    all_neighbors = digraph.neighbors(from_node)
    neighbors = []

    for neighbor in all_neighbors:
        if neighbor in visited:
            continue

        cases_of_neighbor = cases_dic[neighbor]
        overlap_set_with_neighbor = cases_until_node.intersection(cases_of_neighbor)
        overlap_ratio_neighbor = len(overlap_set_with_neighbor) / len(cases_until_node.union(cases_of_neighbor))

        if overlap_ratio_neighbor >= co_path_thresh:
            neighbors.append(neighbor)

    return neighbors


# the function returns two lists
# first list: all paths starting from node from_node
# second list: the common case set of each path in the first list
# visited: start is always already in visited
def find_paths(digraph, cases_dic, co_path_thresh, from_node, cases_until_node, visited, only_maximal):
    paths = []

    neighbors = find_neighbours_with_sufficient_overlap(digraph, cases_dic, co_path_thresh, from_node, cases_until_node, visited)

    if not neighbors:
        return [tuple(visited)]

    for neighbor in neighbors:
        if not only_maximal:
            paths.append((from_node, neighbor))

        cases_of_neighbor = cases_dic[neighbor]
        overlap_set_with_neighbor = cases_until_node.intersection(cases_of_neighbor)
        paths += find_paths(digraph, cases_dic, co_path_thresh, neighbor,
                            overlap_set_with_neighbor, [*visited, neighbor], only_maximal)

    return paths


# the following function returns two lists
# first list: all paths of high-level events
# second list: the common case set of each path in the first list
def hle_co_paths(digraph, case_set_dic, co_thresh, co_path_thresh, maximal_only):

    paths_with_case_overlap = []
    arcs = digraph.edges()

    digraph_copy = digraph.copy()
    # the following checks if the case overlap ratio needed for the paths is stricter (higher) than the one used
    # as case overlap ratio for the event pairs
    # if True: removing more edges as to reduce search space for paths
    if co_path_thresh > co_thresh:
        for arc in arcs:
            common_case_ratio = overlap.case_overlap(arc[0], arc[1], case_set_dic)
            if common_case_ratio < co_path_thresh:
                digraph_copy.remove_edge(*arc)

    nodes = digraph_copy.nodes()

    pool = ThreadPool(processes=2 * os.cpu_count())

    with tqdm(desc='Computing paths', total=len(nodes)) as pbar:
        for paths in pool.imap_unordered(lambda args: find_paths(*args), [
            [digraph_copy, case_set_dic, co_path_thresh, node, case_set_dic[node],
            [node], maximal_only] for node in nodes
        ], chunksize=1):
            paths_with_case_overlap += paths
            pbar.update()

    return paths_with_case_overlap


# returns True only if path2 is an extension of path1, that is, if the first part of path2 is exactly path1
def extends(path1, path2):
    if path1 == path2 or len(path1) > len(path2):
        return False
    else:
        i2 = 0
        for i1 in range(len(path1)):
            if path1[i1] == path2[i2]:
                i1 += 1
                i2 += 1
            else:
                return False

        return True


def get_maximal_paths(hle_paths):
    root = trie.build_trie(hle_paths)
    maximal_paths = trie.find_leaves(root)
    return maximal_paths


# given a sequence of high-level events (which are unique), the function projects each entry onto its high-level
# activity: (f-type, entity), e.g. ('enter', (a,b))
def hle_sequence_to_hla_sequence(hle_sequence, hle_all):
    hla_sequence = []
    for hle in hle_sequence:
        f_type = hle_all[hle]['f-type']
        entity = hle_all[hle]['entity']
        hla = (f_type, entity)
        hla_sequence.append(hla)

    return tuple(hla_sequence)


# the following function returns three lists
# first list: all paths of high-level activities (each hle path is projected onto a hla path)
# second list: the common case set of each hla path in the first list
# third list:
def hla_co_paths_batch(hle_all, i_start, i_end, hle_sequences, case_sequences_of_hle):
    hla_sequences = dict()

    for i in range(i_start, min(i_end, len(hle_sequences))):
        hla_sequence = hle_sequence_to_hla_sequence(hle_sequences[i], hle_all)
        cases_of_sequence_i = case_sequences_of_hle[i]
        if hla_sequence not in hla_sequences:  # the hle path is projected onto a new hla path not already inserted
            hla_sequences[hla_sequence] = (1, cases_of_sequence_i)
        else:  # the hle path is projected onto a hla path that is already inserted in the hla_sequences list
            prev_freq, prev_cases = hla_sequences[hla_sequence]
            hla_sequences[hla_sequence] = (prev_freq + 1), prev_cases.union(cases_of_sequence_i)

    return hla_sequences


def hla_co_paths(hle_all, hle_sequences, case_sequences_of_hle):
    pool = ThreadPool()

    batch_size = 100
    num_batches = int(math.ceil(len(hle_sequences) / batch_size))

    result = dict()

    with tqdm(desc='Parallel hle to hla paths (batches)', total=num_batches) as pbar:
        for partial_result in pool.imap_unordered(lambda params: hla_co_paths_batch(*params), [
            (hle_all, i * batch_size, (i + 1) * batch_size, hle_sequences, case_sequences_of_hle)
            for i in range(num_batches)
        ]):
            for hla_seq, (freq, cases) in partial_result.items():
                prev_freq, prev_cases = result.get(hla_seq, (0, set()))
                result[hla_seq] = prev_freq + freq, cases | prev_cases
            pbar.update()

    return result

