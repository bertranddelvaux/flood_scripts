import pandas as pd

def sum_list_dict(data):
    total = 0
    for d in data:
        for key, value in d.items():
            if isinstance(value, (int, float)):
                total += value
    return total