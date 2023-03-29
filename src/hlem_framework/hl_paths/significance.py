import scipy.stats as stats
import numpy as np


def get_row_and_column_numbers(row_partition, column_partition):
    row_lists = []
    for r, row_cases in enumerate(row_partition):
        row_list = []
        for c, col_cases in enumerate(column_partition):
            rc_intersec = len(row_cases.intersection(col_cases))
            row_list.append(rc_intersec)
        row_lists.append(row_list)
    return np.array(row_lists)


def p_from_fishers_exact_test(table):
    _, p = stats.fisher_exact(table)
    return p


def p_from_chi_square_test(table):
    chi_square = 0
    total_sum = np.sum(table)
    if total_sum == 0:
        return 'No entries'
    row_sums = list(np.sum(table, 1))  # sum of rows
    col_sums = list(np.sum(table, 0))  # sum of rows
    for r, row in enumerate(table):
        row_sum = row_sums[r]
        for c, col in enumerate(row):
            col_sum = col_sums[c]
            obs = table[r][c]
            exp = row_sum*col_sum / total_sum
            if exp > 0:
                chi_square += np.dot((obs-exp)**2, (1/exp))
            else:
                p_val = 1
    freedom = (table.shape[0]-1)*(table.shape[1]-1)
    p_val = 1 - stats.chi2.cdf(chi_square, freedom)
    return p_val


def significance(partition1, partition2, method='chi square'):
    table = get_row_and_column_numbers(partition1, partition2)
    if method == 'chi square':
        p_val = p_from_chi_square_test(table)
    else:
        p_val = p_from_fishers_exact_test(table)
    return p_val, (p_val <= 0.05)
