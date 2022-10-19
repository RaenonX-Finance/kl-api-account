from typing import Any, Callable

import numpy as np
from pandas import DataFrame


def df_fill_na_with_none(func: Callable[[Any], DataFrame]):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df = df.fillna(np.nan).replace(np.nan, None)

        return df

    return wrapper


def df_get_last_non_nan_rev_index(df: DataFrame, columns: list[str]) -> int | None:
    rev_indexes = []

    for column in columns:
        last_non_nan_index_val = df[column].last_valid_index()

        if last_non_nan_index_val is None:
            continue

        rev_indexes.append(df.index.get_loc(last_non_nan_index_val) - len(df) + 1)

    if not rev_indexes:
        return None

    return min(rev_indexes)


def df_get_last_rev_index_of_matching_val(df_base: DataFrame, df_comp: DataFrame, column: str) -> int | None:
    same_val_rev_idx = -1
    same_val_idx_val = df_base.index[same_val_rev_idx]
    while (
        same_val_idx_val not in df_comp.index
        or df_base.at[same_val_idx_val, column] != df_comp.at[same_val_idx_val, column]
    ):
        same_val_rev_idx -= 1

        if same_val_rev_idx + len(df_base) <= 0:
            return None

        same_val_idx_val = df_base.index[same_val_rev_idx]

    return same_val_rev_idx
