import overlap


# the function returns two lists
# first list: all paths starting from node from_node
# second list: the common case set of each path in the first list
def find_paths(digraph, cases_dic, co_path_thresh, from_node, cases_until_node, visited):
    paths = []
    case_sets_of_paths = []
    maximal_paths = []
    case_sets_of_maximal_paths = []
    neighbors = digraph.neighbors(from_node)
    for neighbor in neighbors:
        if neighbor in visited:
            print('Cycle detected')
            continue

        cases_of_neighbor = cases_dic[neighbor]
        overlap_set_with_neighbor = cases_until_node.intersection(cases_of_neighbor)
        overlap_ratio_neighbor = len(overlap_set_with_neighbor) / len(cases_until_node.union(cases_of_neighbor))
        # TODO: maximal paths
        if overlap_ratio_neighbor >= co_path_thresh:
            paths.append([from_node, neighbor])
            case_sets_of_paths.append(overlap_set_with_neighbor)
            paths_from_neigh, case_sets_from_neigh = find_paths(digraph, cases_dic, co_path_thresh, neighbor,
                                                                overlap_set_with_neighbor, [*visited, neighbor])

            for path in paths_from_neigh:
                paths.append([from_node, *path])
            for case_set in case_sets_from_neigh:
                case_sets_of_paths.append(cases_until_node.intersection(case_set))
        else:  # path is maximal
            maximal_paths.append(visited)
            case_sets_of_maximal_paths.append(cases_until_node)

    return paths, case_sets_of_paths


# the following function returns two lists
# first list: all paths of high-level events
# second list: the common case set of each path in the first list
def hle_co_paths(digraph, case_set_dic, co_thresh, co_path_thresh):

    paths_with_case_overlap = []
    cases_of_paths = []
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
    for node in nodes:
        # single nodes are also paths
        paths_with_case_overlap.append([node])
        cases_node = case_set_dic[node]
        cases_of_paths.append(cases_node)
        # all paths from node of at least two elements
        paths_from_node, cases_from_node = find_paths(digraph_copy, case_set_dic, co_path_thresh, node, cases_node,
                                                      [node])
        paths_with_case_overlap += paths_from_node
        cases_of_paths += cases_from_node

    return paths_with_case_overlap, cases_of_paths


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




def get_maximal_paths(hle_paths, hle_cases):
    non_maximal_indices = []
    maximal_paths = []
    maximal_cases = []
    no_hle_paths = len(hle_paths)
    for i, path1 in enumerate(hle_paths):
        print('Checking maximality for ' + str(i) + '/' + str(no_hle_paths) + ' hle paths.')
        for path2 in hle_paths:
            #print("Path1: ", path1)
            #print("Path2: ", path2)
            #print("Is subsequence: ", subsequence_of(path1, path2))
            if extends(path1, path2):
                non_maximal_indices.append(i)

    for i, path in enumerate(hle_paths):
        if i not in non_maximal_indices:
            maximal_paths.append(path)
            maximal_cases.append(hle_cases[i])

    return maximal_paths, maximal_cases


# given a sequence of high-level events (which are unique), the function projects each entry onto its high-level
# activity: (f-type, entity), e.g. ('enter', (a,b))
def hle_sequence_to_hla_sequence(hle_sequence, hle_all):
    hla_sequence = []
    for hle in hle_sequence:
        f_type = hle_all[hle]['f-type']
        entity = hle_all[hle]['entity']
        hla = (f_type, entity)
        hla_sequence.append(hla)
    return hla_sequence


# the following function returns three lists
# first list: all paths of high-level activities (each hle path is projected onto a hla path)
# second list: the common case set of each hla path in the first list
# third list:
def hla_co_paths(hle_all, hle_sequences, case_sequences_of_hle):
    hla_sequences = []
    case_sequences_of_hla = []
    frequencies = []
    no_hle_paths = len(hle_sequences)
    if len(hle_sequences) > 0:

        for i, hle_sequence in enumerate(hle_sequences):
            print('Path ' + str(i) + '/' + str(no_hle_paths))
            hla_sequence = hle_sequence_to_hla_sequence(hle_sequence, hle_all)
            cases_of_sequence_i = case_sequences_of_hle[i]
            if hla_sequence not in hla_sequences:  # the hle path is projected onto a new hla path not already inserted
                hla_sequences.append(hla_sequence)
                case_sequences_of_hla.append(cases_of_sequence_i)
                frequencies.append(1)
            else:  # the hle path is projected onto a hla path that is already inserted in the hla_sequences list
                index = hla_sequences.index(hla_sequence)
                previous_cases = case_sequences_of_hle[index]
                case_sequences_of_hla[index] = previous_cases.union(cases_of_sequence_i)
                frequencies[index] += 1

    return hla_sequences, case_sequences_of_hla, frequencies
