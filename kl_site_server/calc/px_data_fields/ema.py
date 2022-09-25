from typing import Iterable

import talib
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_ema_full(df: DataFrame, periods: Iterable[int]) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df[ema_col_name] = talib.EMA(df[PxDataCol.CLOSE], timeperiod=period)

    return df


def calc_ema_last(df: DataFrame, periods: Iterable[int]) -> DataFrame:
    for period in periods:
        ema_col_name = PxDataCol.get_ema_col_name(period)
        k = 2 / (period + 1)

        df.at[df.index[-1], ema_col_name] = (
            df.at[df.index[-1], PxDataCol.CLOSE] * k
            + df.at[df.index[-2], ema_col_name] * (1 - k)
        )

    return df
