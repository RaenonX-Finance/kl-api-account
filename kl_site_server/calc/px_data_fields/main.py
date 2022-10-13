from typing import Any

from pandas import DataFrame

from kl_site_common.const import INDICATOR_EMA_PERIODS
from kl_site_common.utils import df_fill_na_with_none
from kl_site_server.enums import PxDataCol
from .candlestick import calc_candlestick_full, calc_candlestick_last, calc_candlestick_partial
from .diff import calc_diff_full, calc_diff_last
from .ema import calc_ema_full, calc_ema_last, calc_ema_partial
from .index import calc_set_epoch_index
from .tie_point import calc_tie_point
from ..px_data import aggregate_df


@df_fill_na_with_none
def calculate_indicators_full(period_min: int, data_recs: dict[str, Any]) -> DataFrame:
    df = DataFrame(data_recs)

    calc_set_epoch_index(df)
    df = aggregate_df(df, period_min)

    df = calc_diff_full(df)
    df = calc_tie_point(df, period_min)

    df = calc_candlestick_full(df)
    df = calc_ema_full(df, INDICATOR_EMA_PERIODS)

    return df


@df_fill_na_with_none
def calculate_indicators_partial(period_min: int, data_recs: dict[str, Any], cached_calc_df: DataFrame) -> DataFrame:
    df = DataFrame(data_recs)

    df = aggregate_df(df, period_min)
    calc_set_epoch_index(df)
    calc_set_epoch_index(cached_calc_df)

    df = calc_diff_full(df)
    df = calc_tie_point(df, period_min)

    check_close_index_loc = -1
    check_close_index = cached_calc_df.index[check_close_index_loc]
    while df.at[check_close_index, PxDataCol.CLOSE] != cached_calc_df.at[check_close_index, PxDataCol.CLOSE]:
        check_close_index_loc -= 1
        check_close_index = cached_calc_df.index[check_close_index_loc]

    df = calc_candlestick_partial(df, cached_calc_df, check_close_index_loc)
    df = calc_ema_partial(df, cached_calc_df, check_close_index_loc, INDICATOR_EMA_PERIODS)

    # Partial only calculate until the last of `cached_calc_df`
    # Aggregated `df` could have new timestamp that `cached_calc_df` doesn't have
    df = calculate_indicators_last(period_min, df)

    return df


@df_fill_na_with_none
def calculate_indicators_last(period_min: int, df: DataFrame) -> DataFrame:
    calc_set_epoch_index(df)

    df = calc_diff_last(df)
    df = calc_tie_point(df, period_min)

    df = calc_candlestick_last(df)
    df = calc_ema_last(df, INDICATOR_EMA_PERIODS)

    return df
