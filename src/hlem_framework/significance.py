import math
import pandas as pd
import BPIC_2017_preprocess as bpic
from scipy.stats import fisher_exact
import numpy as np


def get_p_value(a, b, c, d, n):
    over = math.factorial(a + b) * math.factorial(c + d) * math.factorial(a + c) * math.factorial(b + d)
    under = math.factorial(a) * math.factorial(b) * math.factorial(c) * math.factorial(d) * math.factorial(n)
    p = over / under

    return p


def get_table(participating, non_participating, successful, not_successful):
    a_set = participating.intersection(successful)
    a = len(a_set)
    b_set = non_participating.intersection(successful)
    b = len(b_set)
    c_set = participating.intersection(not_successful)
    # print(len(a_set))
    c = len(c_set)
    d_set = non_participating.intersection(not_successful)
    d = len(d_set)
    table = np.array([[a, b], [c, d]])
    return table

def get_path_numbers(participating, non_participating, successful, not_successful):
    a_set = participating.intersection(successful)
    #print(len(a_set))
    a = len(a_set)
    b_set = non_participating.intersection(successful)
    b = len(b_set)
    c_set = participating.intersection(not_successful)
    #print(len(a_set))
    c = len(c_set)
    d_set = non_participating.intersection(not_successful)
    d = len(d_set)
    n = a + b + c + d
    p_value = get_p_value(a, b, c, d, n)
    return a, b, c, d, p_value


file_name = 'BPM-23-short-2.csv'
df = pd.read_csv(file_name, converters={'path': eval, 'relevant cases': eval, 'participating': eval, 'non-participating': eval})
log, res_selection, success_cases, non_success_cases, cases_under_10, cases_10_to_30, cases_over_30 = bpic.preprocess_bpic_2017()

# row contains path, length, a, b, c, d, p-value, p valid (< )
path = []
participating = set()
non_participating = set()
successful = set()
not_successful = set()

for i in range(len(df)):
    # print(i)
    path = df.iloc[i]['path']
    participating = df.iloc[i]['participating']
    #print(len(participating))
    non_participating = df.iloc[i]['non-participating']
    #print(len(non_participating))
    #a, b, c, d, p_value = get_path_numbers(participating, non_participating, success_cases, non_success_cases)
    table = get_table(participating, non_participating, success_cases, non_success_cases)
    _, p_value = fisher_exact(table)
    print(p_value)
    if p_value < 0.05:
        print('p_value: ' + str(p_value))
        #print(a, b, c, d)
        print(len(path))
        print(path)



