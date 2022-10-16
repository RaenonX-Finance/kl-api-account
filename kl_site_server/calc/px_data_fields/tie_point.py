from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame

from kl_site_common.utils import df_get_last_non_nan_rev_index
from kl_site_server.enums import PxDataCol


def _tie_point_all_na(df: DataFrame) -> DataFrame:
    df[PxDataCol.MARKET_DATE_HIGH] = np.full([len(df)], np.nan)
    df[PxDataCol.MARKET_DATE_LOW] = np.full([len(df)], np.nan)
    df[PxDataCol.TIE_POINT] = np.full([len(df)], np.nan)

    return df


def calc_tie_point_full(df: DataFrame, period_min: int) -> DataFrame:
    if period_min >= 1440:  # Period > 1 day, tie point is meaningless
        return _tie_point_all_na(df)

    mkt_date_group = df.groupby(PxDataCol.DATE_MARKET)

    df[PxDataCol.MARKET_DATE_HIGH] = mkt_date_group[PxDataCol.HIGH].transform(pd.Series.cummax)
    df[PxDataCol.MARKET_DATE_LOW] = mkt_date_group[PxDataCol.LOW].transform(pd.Series.cummin)
    df[PxDataCol.TIE_POINT] = np.add(df[PxDataCol.MARKET_DATE_HIGH], df[PxDataCol.MARKET_DATE_LOW]) / 2

    return df


def _tie_point_of_index(df: DataFrame, idx_curr: Any, idx_prev: Any) -> DataFrame:
    cur_close = df.at[idx_curr, PxDataCol.CLOSE]

    if df.at[idx_curr, PxDataCol.DATE_MARKET] != df.at[idx_prev, PxDataCol.DATE_MARKET]:
        df.at[idx_curr, PxDataCol.MARKET_DATE_HIGH] = cur_close
        df.at[idx_curr, PxDataCol.MARKET_DATE_LOW] = cur_close
        df.at[idx_curr, PxDataCol.TIE_POINT] = cur_close
    else:
        df.at[idx_curr, PxDataCol.MARKET_DATE_HIGH] = hi = max(
            df.at[idx_prev, PxDataCol.MARKET_DATE_HIGH], cur_close
        )
        df.at[idx_curr, PxDataCol.MARKET_DATE_LOW] = lo = min(
            df.at[idx_prev, PxDataCol.MARKET_DATE_LOW], cur_close
        )
        df.at[idx_curr, PxDataCol.TIE_POINT] = (hi + lo) / 2

    return df


def calc_tie_point_partial(
    df: DataFrame, cached_calc_df: DataFrame, close_match_rev_idx_on_df: int, period_min: int
) -> DataFrame:
    if period_min >= 1440:  # Period > 1 day, tie point is meaningless
        return _tie_point_all_na(df)

    df[PxDataCol.MARKET_DATE_HIGH] = cached_calc_df[PxDataCol.MARKET_DATE_HIGH].copy()
    df[PxDataCol.MARKET_DATE_LOW] = cached_calc_df[PxDataCol.MARKET_DATE_LOW].copy()
    df[PxDataCol.TIE_POINT] = cached_calc_df[PxDataCol.TIE_POINT].copy()

    nan_rev_index = df_get_last_non_nan_rev_index(df, [PxDataCol.MARKET_DATE_HIGH, PxDataCol.MARKET_DATE_LOW])

    for base_index in range(min(close_match_rev_idx_on_df, nan_rev_index or 0), 0):
        df = _tie_point_of_index(df, df.index[base_index], df.index[base_index - 1])

    return df


def calc_tie_point_last(df: DataFrame, period_min: int) -> DataFrame:
    if period_min >= 1440:
        df.at[df.index[-1], PxDataCol.TIE_POINT] = np.nan

        return df

    return _tie_point_of_index(df, df.index[-1], df.index[-2])
