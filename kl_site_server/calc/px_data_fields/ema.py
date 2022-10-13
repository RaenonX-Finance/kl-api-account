from typing import Iterable

import talib
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_ema_single(current: float, prev_ema: float, period: int) -> float:
    k = 2 / (period + 1)
    return current * k + prev_ema * (1 - k)


def calc_ema_full(df: DataFrame, periods: Iterable[int]) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df[ema_col_name] = talib.EMA(df[PxDataCol.CLOSE], timeperiod=period)

    return df


def calc_ema_partial(
    df: DataFrame, cached_calc_df: DataFrame, close_match_rev_index: int, periods: Iterable[int],
) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df[ema_col_name] = cached_calc_df[ema_col_name].copy()

        for base_index in range(close_match_rev_index, 0):
            last_ema = df.at[df.index[base_index - 1], ema_col_name]

            if last_ema:
                df.at[df.index[base_index], ema_col_name] = calc_ema_single(
                    df.at[df.index[base_index], PxDataCol.CLOSE], last_ema, period
                )
            else:
                df.at[df.index[base_index], ema_col_name] = None

    return df


def calc_ema_last(df: DataFrame, periods: Iterable[int]) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)
        last_ema = df.at[df.index[-2], ema_col_name]

        if last_ema:
            df.at[df.index[-1], ema_col_name] = calc_ema_single(
                df.at[df.index[-1], PxDataCol.CLOSE], last_ema, period
            )
        else:
            df.at[df.index[-1], ema_col_name] = None

    return df
