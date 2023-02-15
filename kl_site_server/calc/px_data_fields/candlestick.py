from typing import Any

import numpy as np
import talib
from pandas import DataFrame, Series

from kl_site_common.utils import df_get_last_non_nan_rev_index
from kl_site_server.db import PX_CONFIG
from kl_site_server.enums import PxDataCol
from .ema import calc_ema_single

_px_col_macd_fast = PxDataCol.get_ema_col_name(PX_CONFIG.candle_dir.fast)
_px_col_macd_slow = PxDataCol.get_ema_col_name(PX_CONFIG.candle_dir.slow)


def _macd_full(px: Series) -> tuple[Series, Series, Series, Series]:
    # Creating formula instead of using `talib.MACD()`
    # because the aforementioned function returns incorrect result
    macd_fast = talib.EMA(px, timeperiod=PX_CONFIG.candle_dir.fast)
    macd_slow = talib.EMA(px, timeperiod=PX_CONFIG.candle_dir.slow)
    macd = macd_fast - macd_slow
    sig = talib.EMA(macd, timeperiod=PX_CONFIG.candle_dir.signal)
    hist = macd - sig

    return macd_fast, macd_slow, sig, hist


def calc_candlestick_full(df: DataFrame) -> DataFrame:
    macd_fast, macd_slow, signal, hist = _macd_full(df[PxDataCol.CLOSE])

    df[_px_col_macd_fast] = macd_fast
    df[_px_col_macd_slow] = macd_slow
    df[PxDataCol.MACD_SIGNAL] = signal
    df[PxDataCol.CANDLESTICK_DIR] = np.where(
        np.isnan(hist),
        0,
        np.where(hist > 0, 1, -1)
    )

    return df


def _macd_of_index(df: DataFrame, idx_curr: Any, idx_prev: Any) -> DataFrame:
    cur_close = df.at[idx_curr, PxDataCol.CLOSE]

    macd_fast = calc_ema_single(cur_close, df.at[idx_prev, _px_col_macd_fast], PX_CONFIG.candle_dir.fast)
    macd_slow = calc_ema_single(cur_close, df.at[idx_prev, _px_col_macd_slow], PX_CONFIG.candle_dir.slow)

    macd = macd_fast - macd_slow
    sig = calc_ema_single(macd, df.at[idx_prev, PxDataCol.MACD_SIGNAL], PX_CONFIG.candle_dir.signal)
    hist = macd - sig

    df.at[idx_curr, _px_col_macd_fast] = macd_fast
    df.at[idx_curr, _px_col_macd_slow] = macd_slow
    df.at[idx_curr, PxDataCol.MACD_SIGNAL] = sig
    df.at[idx_curr, PxDataCol.CANDLESTICK_DIR] = np.where(np.isnan(hist), 0, np.where(hist > 0, 1, -1))

    return df


def calc_candlestick_partial(df: DataFrame, cached_calc_df: DataFrame, close_match_rev_idx_on_df: int) -> DataFrame:
    df[_px_col_macd_fast] = cached_calc_df[_px_col_macd_fast].copy()
    df[_px_col_macd_slow] = cached_calc_df[_px_col_macd_slow].copy()
    df[PxDataCol.MACD_SIGNAL] = cached_calc_df[PxDataCol.MACD_SIGNAL].copy()
    df[PxDataCol.CANDLESTICK_DIR] = cached_calc_df[PxDataCol.CANDLESTICK_DIR].copy()

    nan_rev_index = df_get_last_non_nan_rev_index(
        df, [_px_col_macd_fast, _px_col_macd_slow, PxDataCol.MACD_SIGNAL, PxDataCol.CANDLESTICK_DIR]
    )
    for base_index in range(min(close_match_rev_idx_on_df, nan_rev_index or 0), 0):
        df = _macd_of_index(df, df.index[base_index], df.index[base_index - 1])

    return df


def calc_candlestick_last(df: DataFrame) -> DataFrame:
    return _macd_of_index(df, df.index[-1], df.index[-2])
