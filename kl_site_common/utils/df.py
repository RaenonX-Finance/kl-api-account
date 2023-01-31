import time
from datetime import datetime
from typing import Any, Callable

import numpy as np
from pandas import DataFrame, to_datetime

from kl_site_common.utils import print_log


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


def df_load_entries_with_dt(data: list[dict], *, df_name: str | None = None) -> DataFrame:
    if not data:
        raise ValueError("No data available for creating dataframe")

    _start = time.time()

    data_col_names_dt = {k for k, v in data[0].items() if isinstance(v, datetime)}
    data_new = [
        {
            key: value.isoformat() if key in data_col_names_dt else value
            for key, value in entry.items()
        }
        for entry in data
    ]

    df = DataFrame.from_records(data_new)
    for dt_col_name in data_col_names_dt:
        df[dt_col_name] = to_datetime(df[dt_col_name])

    print_log(
        f"Created DT dataframe of {f' x '.join(str(d) for d in df.shape)} in {time.time() - _start:.3f} s "
        f"({df_name or 'Unnamed'})"
    )

    return df
