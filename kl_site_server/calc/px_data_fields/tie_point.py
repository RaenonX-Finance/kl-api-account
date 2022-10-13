import numpy as np
import pandas as pd
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def _tie_point_all_na(df: DataFrame) -> DataFrame:
    df[PxDataCol.MARKET_DATE_HIGH] = np.empty(len(df))
    df[PxDataCol.MARKET_DATE_LOW] = np.empty(len(df))
    df[PxDataCol.TIE_POINT] = np.empty(len(df))

    return df


def calc_tie_point_full(df_1k: DataFrame, period_min: int) -> DataFrame:
    if period_min >= 1440:  # Period > 1 day, tie point is meaningless
        return _tie_point_all_na(df_1k)

    mkt_date_group = df_1k.groupby(PxDataCol.DATE_MARKET)

    df_1k[PxDataCol.MARKET_DATE_HIGH] = mkt_date_group[PxDataCol.HIGH].transform(pd.Series.cummax)
    df_1k[PxDataCol.MARKET_DATE_LOW] = mkt_date_group[PxDataCol.LOW].transform(pd.Series.cummin)
    df_1k[PxDataCol.TIE_POINT] = np.add(df_1k[PxDataCol.MARKET_DATE_HIGH], df_1k[PxDataCol.MARKET_DATE_LOW]) / 2

    return df_1k


def _tie_point_of_index(df: DataFrame, cached_calc_df: DataFrame, base_index: int) -> DataFrame:
    idx_curr = cached_calc_df.index[base_index]
    idx_prev = cached_calc_df.index[base_index - 1]

    cur_close = df.at[idx_curr, PxDataCol.CLOSE]

    if df.at[idx_curr, PxDataCol.DATE_MARKET] != df.at[idx_prev, PxDataCol.DATE_MARKET]:
        df.at[idx_curr, PxDataCol.MARKET_DATE_HIGH] = cur_close
        df.at[idx_curr, PxDataCol.MARKET_DATE_LOW] = cur_close
        df.at[idx_curr, PxDataCol.TIE_POINT] = cur_close
    else:
        df.at[idx_curr, PxDataCol.MARKET_DATE_HIGH] = hi = max(
            df.at[idx_curr, PxDataCol.MARKET_DATE_HIGH], cur_close
        )
        df.at[idx_curr, PxDataCol.MARKET_DATE_LOW] = lo = min(
            df.at[idx_curr, PxDataCol.MARKET_DATE_LOW], cur_close
        )
        df.at[idx_curr, PxDataCol.TIE_POINT] = (hi + lo) / 2

    return df


def calc_tie_point_partial(
    df: DataFrame, cached_calc_df: DataFrame, close_match_rev_index: int, period_min: int
) -> DataFrame:
    if period_min >= 1440:  # Period > 1 day, tie point is meaningless
        return _tie_point_all_na(df)

    df[PxDataCol.MARKET_DATE_HIGH] = cached_calc_df[PxDataCol.MARKET_DATE_HIGH].copy()
    df[PxDataCol.MARKET_DATE_LOW] = cached_calc_df[PxDataCol.MARKET_DATE_LOW].copy()
    df[PxDataCol.TIE_POINT] = cached_calc_df[PxDataCol.TIE_POINT].copy()

    for base_index in range(close_match_rev_index, 0):
        df = _tie_point_of_index(df, cached_calc_df, base_index)

    return df


def calc_tie_point_last(df: DataFrame, period_min: int) -> DataFrame:
    if period_min >= 1440:
        df.at[df.index[-1], PxDataCol.TIE_POINT] = np.nan

        return df

    return _tie_point_of_index(df, df, -1)
