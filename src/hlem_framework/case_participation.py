from collections import defaultdict
import high_level_paths as paths
import networkx as nx


def hle_set_by_case_dict(case_set_dic):

    hle_set_of_case_dic = defaultdict(lambda: [])
    for hle_id in case_set_dic.keys():
        cases_of_hle = case_set_dic[hle_id]
        for case in cases_of_hle:
            hle_set_of_case_dic[case].append(hle_id)

    return hle_set_of_case_dic


def is_subsequence(activity_sequence, trace):
    if len(activity_sequence) > len(trace):
        return False
    elif activity_sequence == trace:
        return True
    else:
        seq_start = activity_sequence[0]
        # find all entries in trace identical to first one in the activity sequence
        # for each entry, if the trace from that entry is an extension of the activity sequence, return True
        # else continue with next entry
        start_indices = [i for i, act in enumerate(trace) if act == seq_start]
        for index in start_indices:
            trace_from_index = trace[index:]
            if paths.extends(activity_sequence, trace_from_index):
                return True
        # if finished without finding a proper extension, return False
        return False


def is_subsequence_fast(activity_sequence, trace):
    act_seq_str = ' '.join([str(act) for act in activity_sequence])
    trace_str = ' '.join([str(act) for act in trace])
    if len(activity_sequence) > len(trace):
        return False
    else:
        return act_seq_str in trace_str


def project_path_onto_activity_sequence(hla_path):
    first_hla = hla_path[0]
    first_segment = first_hla[1]
    first_activity = first_segment[0]
    projection = [first_activity]
    #TODO safe check if the intersecting activities are not identical
    for hla in hla_path:
        segment = hla[1]
        second_activity = segment[1]
        projection.append(second_activity)
    return projection


def get_cf_dict(log):
    cf_dict = dict()
    for trace in log:
        case_id = trace.attributes['concept:name']
        control_flow = [event['concept:name'] for event in trace]
        cf_dict[case_id] = control_flow
    return cf_dict


# returns set of case ids of all cases that traverse an activity sequence (underlying a hla path)
def get_case_pool(cf_dict, subsequence):
    relevant_cases = []
    for case_id in cf_dict.keys():
        control_flow = cf_dict[case_id]
        if is_subsequence_fast(subsequence, control_flow):
            relevant_cases.append(case_id)

    return set(relevant_cases)


if __name__ == '__main__':
    digraph = nx.DiGraph()
    nodes = ['A', 'B', 'C', 'D', 'E']
    arcs = [('A', 'B'), ('A', 'C'), ('A', 'D'), ('C', 'D'), ('D', 'E'), ('D', 'F')]
    digraph.add_nodes_from(nodes)
    digraph.add_edges_from(arcs)
    cases_dict = dict()
    cases_dict['A'] = {1, 2, 3, 4}
    cases_dict['B'] = {1, 2}
    cases_dict['C'] = {1, 3, 4}
    cases_dict['D'] = {1, 3, 4, 6}
    cases_dict['E'] = {1, 6}
    cases_dict['F'] = {3, 6}

    co_thresh = 0.5
    co_path_thresh = 0.5

    hle_sequences, hle_cases = paths.hle_co_paths(digraph, cases_dict, co_thresh, co_path_thresh)
    hle_sequences, hle_cases = paths.get_maximal_paths(hle_sequences, hle_cases)
    print(hle_sequences)
    print(hle_cases)

    hle_all = defaultdict(lambda: {})
    hle_all['A']['f-type'] = 'enter'
    hle_all['A']['entity'] = 'x'
    hle_all['B']['f-type'] = 'exit'
    hle_all['B']['entity'] = 'y'
    hle_all['C']['f-type'] = 'exit'
    hle_all['C']['entity'] = 'y'
    hle_all['D']['f-type'] = 'workload'
    hle_all['D']['entity'] = 'z'
    hle_all['E']['f-type'] = 'handover'
    hle_all['E']['entity'] = 'x'
    hle_all['F']['f-type'] = 'enter'
    hle_all['F']['entity'] = 'y'

    hla_sequences, case_sequences_of_hla, frequencies = paths.hla_co_paths(hle_all, hle_sequences, hle_cases)
    print(hla_sequences)
    print(case_sequences_of_hla)
