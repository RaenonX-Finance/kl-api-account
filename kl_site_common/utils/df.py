from typing import Any, Callable

import numpy as np
from pandas import DataFrame


def df_fill_na_with_none(func: Callable[[Any], DataFrame]):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df = df.fillna(np.nan).replace(np.nan, None)

        return df

    return wrapper


def df_get_last_non_nan_rev_index(df: DataFrame, columns: list[str]) -> int:
    rev_indexes = []

    for column in columns:
        last_non_nan_index_val = df[column].last_valid_index()
        rev_indexes.append(df.index.get_loc(last_non_nan_index_val) - len(df) + 1)

    return min(rev_indexes)
