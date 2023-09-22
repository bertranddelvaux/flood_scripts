import pandas as pd
import numpy as np

from constants.constants import AGREEMENT_THRESHOLD

def sum_list_dict(data):
    total = 0
    for d in data:
        for key, value in d.items():
            if isinstance(value, (int, float)):
                total += value
    return total

def find_maximum_values(df_A, df_B):
    # Identify non-numeric columns to join on
    join_cols = list(set(df_A.columns) & set(df_B.columns))
    numeric_columns = [col for col in df_A.columns if pd.api.types.is_numeric_dtype(df_A[col])]
    non_numeric_colums = [col for col in join_cols if col not in numeric_columns]

    df_merged = pd.merge(df_A, df_B, how='outer', on=non_numeric_colums, suffixes=['_x', '_y']).fillna(value=0)

    # for each numeric column, take the maximum of _x, _y
    for col in numeric_columns:
        df_merged[col] = df_merged[[f'{col}_x', f'{col}_y']].max(axis=1).astype(int)
        # Remove the other two columns
        df_merged.drop(columns=[f'{col}_x', f'{col}_y'], inplace=True)

    return df_merged


def agg_threshold(series, threshold: float = AGREEMENT_THRESHOLD):
    """
    Custom aggregation function to calculate the mode of a series
    :param series:
    :return:
    """

    # Calculate the mode (most common value)
    mode = series.mode().iloc[0]

    # Calculate the count of the mode value
    mode_count = (series == mode).sum()

    # If the mode appears at least 50% of the time, return the mode, else return 0
    if mode_count >= int(np.round(len(series) * threshold)):
        return int(np.round(mode))
    else:
        return 0
