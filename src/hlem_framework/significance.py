import math
import pandas as pd


def significance(class_dict, relevant_cases, participating_cases, non_participating_cases):
    pass


def get_p_value(a, b, c, d, n):
    over = math.factorial(a + b) * math.factorial(c + d) * math.factorial(a + c) * math.factorial(b + d)
    under = math.factorial(a) * math.factorial(b) * math.factorial(c) * math.factorial(d) * math.factorial(n)
    p = over / under

    return p


def get_path_numbers(participating, non_participating, successful, not_successful):
    a_set = participating.intersection(successful)
    a = len(a_set)
    b_set = non_participating.intersection(successful)
    b = len(b_set)
    c_set = participating.intersection(not_successful)
    c = len(c_set)
    d_set = non_participating.intersection(not_successful)
    d = len(d_set)
    n = a + b + c + d
    p_value = get_p_value(a, b, c, d, n)
    return a, b, c, d, p_value


# row contains path, length, a, b, c, d, p-value, p valid (< )
path = []
participating = set()
non_participating = set()
successful = set()
not_successful = set()


df_row = []
length = len(path)
a, b, c, d, p_value = get_path_numbers(participating, non_participating, successful, not_successful)
if p_value < 0.05:
    valid = True
else:
    valid = False
df_row = [path, length, a, b, c, d, p_value, valid]


