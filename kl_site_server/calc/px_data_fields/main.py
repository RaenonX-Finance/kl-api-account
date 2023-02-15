from pandas import DataFrame

from kl_site_common.utils import df_fill_na_with_none, df_get_last_rev_index_of_matching_val, print_log
from kl_site_server.db import PX_CONFIG
from kl_site_server.enums import PxDataCol
from .candlestick import calc_candlestick_full, calc_candlestick_last, calc_candlestick_partial
from .diff import calc_diff_full, calc_diff_last
from .ema import calc_ema_full, calc_ema_last, calc_ema_partial
from .index import calc_set_epoch_index
from .tie_point import calc_tie_point_full, calc_tie_point_last, calc_tie_point_partial
from ..px_data import aggregate_df


class CachedDataTooOldError(Exception):
    pass


@df_fill_na_with_none
def calculate_indicators_full(period_min: int, df: DataFrame) -> DataFrame:
    df = aggregate_df(df, period_min)

    df = calc_diff_full(df)
    df = calc_tie_point_full(df, period_min)

    df = calc_candlestick_full(df)
    df = calc_ema_full(df, PX_CONFIG.ema_periods)

    return df


@df_fill_na_with_none
def calculate_indicators_partial(
    security: str, period_min: int, df: DataFrame, cached_calc_df: DataFrame
) -> DataFrame:
    df = aggregate_df(df, period_min)
    calc_set_epoch_index(df)
    calc_set_epoch_index(cached_calc_df)

    # `df` may have data older than the earliest data of `cached_calc_df`
    # This ensures `df` starts from the same starting point of `cached_calc_df`
    # Removing this creates incorrect `NaN`s in many columns
    df = df[cached_calc_df.index[0]:]

    if len(df) < 2:
        raise ValueError(
            f"`{security}@{period_min}` does not have enough data to do partial calculation - "
            f"Base: {df.index[0]} ~ {df.index[-1]} / Cached: {cached_calc_df.index[0]} ~ {cached_calc_df.index[-1]}"
        )

    df = calc_diff_full(df)

    close_match_idx_on_df = df_get_last_rev_index_of_matching_val(df, cached_calc_df, PxDataCol.CLOSE)
    if not close_match_idx_on_df:
        raise CachedDataTooOldError()

    print_log(f"Closing Px match at {close_match_idx_on_df} for [yellow]{security}@{period_min}[/]")

    df = calc_tie_point_partial(df, cached_calc_df, close_match_idx_on_df, period_min)

    df = calc_candlestick_partial(df, cached_calc_df, close_match_idx_on_df)
    df = calc_ema_partial(df, cached_calc_df, close_match_idx_on_df, PX_CONFIG.ema_periods)

    # Partial only calculate until the last of `cached_calc_df`
    # Aggregated `df` could have new timestamp that `cached_calc_df` doesn't have
    df = calculate_indicators_last(period_min, df)

    # Only return "possibly changed" data
    df = df.iloc[close_match_idx_on_df - 1:]

    return df


@df_fill_na_with_none
def calculate_indicators_last(period_min: int, df: DataFrame) -> DataFrame:
    calc_set_epoch_index(df)

    df = calc_diff_last(df)
    df = calc_tie_point_last(df, period_min)

    df = calc_candlestick_last(df)
    df = calc_ema_last(df, PX_CONFIG.ema_periods)

    return df
