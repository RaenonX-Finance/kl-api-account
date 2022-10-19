import numpy as np
import talib
from pandas import DataFrame, Series

from kl_site_server.enums import PxDataCol
from kl_site_server.utils import CANDLESTICK_DIR_MACD_FAST, CANDLESTICK_DIR_MACD_SIGNAL, CANDLESTICK_DIR_MACD_SLOW
from .ema import calc_ema_single


_px_col_macd_fast = PxDataCol.get_ema_col_name(CANDLESTICK_DIR_MACD_FAST)
_px_col_macd_slow = PxDataCol.get_ema_col_name(CANDLESTICK_DIR_MACD_SLOW)


def _macd_full(px: Series) -> tuple[Series, Series, Series, Series]:
    # Creating formula instead of using `talib.MACD()`
    # because the aforementioned function returns incorrect result
    macd_fast = talib.EMA(px, timeperiod=CANDLESTICK_DIR_MACD_FAST)
    macd_slow = talib.EMA(px, timeperiod=CANDLESTICK_DIR_MACD_SLOW)
    macd = macd_fast - macd_slow
    sig = talib.EMA(macd, timeperiod=CANDLESTICK_DIR_MACD_SIGNAL)
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


def _macd_of_index(df: DataFrame, df_ema_base: DataFrame, *, base_index: int) -> tuple[float, float, float, float]:
    cur_close = df.at[df_ema_base.index[base_index], PxDataCol.CLOSE]

    macd_fast = calc_ema_single(
        cur_close,
        df_ema_base.at[df_ema_base.index[base_index - 1], PxDataCol.get_ema_col_name(CANDLESTICK_DIR_MACD_FAST)],
        CANDLESTICK_DIR_MACD_FAST
    )
    macd_slow = calc_ema_single(
        cur_close,
        df_ema_base.at[df_ema_base.index[base_index - 1], PxDataCol.get_ema_col_name(CANDLESTICK_DIR_MACD_SLOW)],
        CANDLESTICK_DIR_MACD_SLOW
    )

    macd = macd_fast - macd_slow
    sig = calc_ema_single(
        macd,
        df_ema_base.at[df_ema_base.index[base_index - 1], PxDataCol.MACD_SIGNAL],
        CANDLESTICK_DIR_MACD_SIGNAL
    )
    hist = macd - sig

    return macd_fast, macd_slow, sig, hist


def calc_candlestick_partial(df: DataFrame, cached_calc_df: DataFrame, close_match_rev_index: int) -> DataFrame:
    df[_px_col_macd_fast] = cached_calc_df[_px_col_macd_fast].copy()
    df[_px_col_macd_slow] = cached_calc_df[_px_col_macd_slow].copy()
    df[PxDataCol.MACD_SIGNAL] = cached_calc_df[PxDataCol.MACD_SIGNAL].copy()
    df[PxDataCol.CANDLESTICK_DIR] = cached_calc_df[PxDataCol.CANDLESTICK_DIR].copy()

    for base_index in range(close_match_rev_index, 0):
        macd_fast, macd_slow, sig, hist = _macd_of_index(df, cached_calc_df, base_index=base_index)

        df.at[cached_calc_df.index[base_index], _px_col_macd_fast] = macd_fast
        df.at[cached_calc_df.index[base_index], _px_col_macd_slow] = macd_slow
        df.at[cached_calc_df.index[base_index], PxDataCol.MACD_SIGNAL] = sig
        df.at[cached_calc_df.index[base_index], PxDataCol.CANDLESTICK_DIR] = np.where(
            np.isnan(hist),
            0,
            np.where(hist > 0, 1, -1)
        )

    return df


def _macd_last(df: DataFrame) -> tuple[float, float, float, float]:
    return _macd_of_index(df, df, base_index=-1)


def calc_candlestick_last(df: DataFrame) -> DataFrame:
    macd_fast, macd_slow, signal, hist = _macd_last(df)

    df.at[df.index[-1], _px_col_macd_fast] = macd_fast
    df.at[df.index[-1], _px_col_macd_slow] = macd_slow
    df.at[df.index[-1], PxDataCol.MACD_SIGNAL] = signal
    df.at[df.index[-1], PxDataCol.CANDLESTICK_DIR] = np.where(
        np.isnan(hist),
        0,
        np.where(hist > 0, 1, -1)
    )

    return df
