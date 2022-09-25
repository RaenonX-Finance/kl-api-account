from typing import Any

import numpy as np
from pandas import DataFrame

from kl_site_common.const import INDICATOR_EMA_PERIODS
from .candlestick import calc_candlestick
from .diff import calc_diff
from .ema import calc_ema_full, calc_ema_last
from .index import calc_set_epoch_index
from .tie_point import calc_tie_point
from ..px_data import aggregate_df


def df_fill_na(func):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df = df.fillna(np.nan)

        return df

    return wrapper


@df_fill_na
def calculate_indicators_full(period_min: int, data_recs: dict[str, Any]) -> DataFrame:
    df = DataFrame(data_recs)

    calc_set_epoch_index(df)
    df = aggregate_df(df, period_min)

    df = calc_diff(df)
    df = calc_candlestick(df)
    df = calc_tie_point(df, period_min)
    df = calc_ema_full(df, INDICATOR_EMA_PERIODS)

    return df


@df_fill_na
def calculate_indicators_partial(period_min: int, df: DataFrame) -> DataFrame:
    calc_set_epoch_index(df)

    df = calc_diff(df)
    df = calc_candlestick(df)
    df = calc_tie_point(df, period_min)
    df = calc_ema_last(df, INDICATOR_EMA_PERIODS)

    return df
