from typing import Any, Iterable

import numpy as np
import talib
from pandas import DataFrame

from kl_site_common.utils import df_get_last_non_nan_rev_index
from kl_site_server.enums import PxDataCol


def calc_ema_single(current: float, prev_ema: float, period: int) -> float:
    k = 2 / (period + 1)
    return current * k + prev_ema * (1 - k)


def calc_ema_full(df: DataFrame, periods: Iterable[int]) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df[ema_col_name] = talib.EMA(df[PxDataCol.CLOSE], timeperiod=period)

    return df


def _ema_of_index(
    df: DataFrame, idx_curr: Any, idx_prev: Any, ema_col_name: str, period: int
) -> DataFrame:
    last_ema = df.at[idx_prev, ema_col_name]

    if last_ema:
        df.at[idx_curr, ema_col_name] = calc_ema_single(
            df.at[idx_curr, PxDataCol.CLOSE], last_ema, period
        )
    else:
        df.at[idx_curr, ema_col_name] = np.nan

    return df


def calc_ema_partial(
    df: DataFrame, df_ema_base: DataFrame, close_match_rev_idx_on_df: int, periods: Iterable[int],
) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df[ema_col_name] = df_ema_base[ema_col_name].copy()

        nan_rev_index = df_get_last_non_nan_rev_index(df, [ema_col_name])

        for base_index in range(min(close_match_rev_idx_on_df, nan_rev_index or 0), 0):
            df = _ema_of_index(
                df,
                df.index[base_index],
                df.index[base_index - 1],
                ema_col_name,
                period
            )

    return df


def calc_ema_last(df: DataFrame, periods: Iterable[int]) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df = _ema_of_index(df, df.index[-1], df.index[-2], ema_col_name, period)

    return df
