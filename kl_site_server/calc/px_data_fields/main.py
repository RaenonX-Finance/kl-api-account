from typing import Any

from pandas import DataFrame

from kl_site_common.const import INDICATOR_EMA_PERIODS
from kl_site_common.utils import df_fill_na_with_none, df_get_last_rev_index_of_matching_val
from kl_site_server.enums import PxDataCol
from .candlestick import calc_candlestick_full, calc_candlestick_last, calc_candlestick_partial
from .diff import calc_diff_full, calc_diff_last
from .ema import calc_ema_full, calc_ema_last, calc_ema_partial
from .index import calc_set_epoch_index
from .tie_point import calc_tie_point_full, calc_tie_point_partial, calc_tie_point_last
from ..px_data import aggregate_df


@df_fill_na_with_none
def calculate_indicators_full(period_min: int, data_recs: dict[str, Any]) -> DataFrame:
    df = DataFrame(data_recs)

    calc_set_epoch_index(df)
    df = aggregate_df(df, period_min)

    df = calc_diff_full(df)
    df = calc_tie_point_full(df, period_min)

    df = calc_candlestick_full(df)
    df = calc_ema_full(df, INDICATOR_EMA_PERIODS)

    return df


@df_fill_na_with_none
def calculate_indicators_partial(period_min: int, data_recs: dict[str, Any], cached_calc_df: DataFrame) -> DataFrame:
    df = DataFrame(data_recs)

    df = aggregate_df(df, period_min)
    calc_set_epoch_index(df)
    calc_set_epoch_index(cached_calc_df)

    # `df` may have data older than the earliest data of `cached_calc_df`
    # This ensures `df` starts from the same starting point of `cached_calc_df`
    # Removing this creates incorrect `NaN`s in many columns
    df = df[cached_calc_df.index[0]:]

    df = calc_diff_full(df)

    close_match_idx_on_df = df_get_last_rev_index_of_matching_val(df, cached_calc_df, PxDataCol.CLOSE)
    if not close_match_idx_on_df:
        close_match_idx_on_df = -len(df) + 1

    df = calc_tie_point_partial(df, cached_calc_df, close_match_idx_on_df, period_min)

    df = calc_candlestick_partial(df, cached_calc_df, close_match_idx_on_df)
    df = calc_ema_partial(df, cached_calc_df, close_match_idx_on_df, INDICATOR_EMA_PERIODS)

    # Partial only calculate until the last of `cached_calc_df`
    # Aggregated `df` could have new timestamp that `cached_calc_df` doesn't have
    df = calculate_indicators_last(period_min, df)

    return df


@df_fill_na_with_none
def calculate_indicators_last(period_min: int, df: DataFrame) -> DataFrame:
    calc_set_epoch_index(df)

    df = calc_diff_last(df)
    df = calc_tie_point_last(df, period_min)

    df = calc_candlestick_last(df)
    df = calc_ema_last(df, INDICATOR_EMA_PERIODS)

    return df
