import numpy as np
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def _calc_strength(df_1k: DataFrame, short_period: int, long_period: int) -> int:
    last_close = df_1k[PxDataCol.CLOSE].iat[-1]
    avg_short = np.mean(df_1k[PxDataCol.CLOSE].tail(short_period))
    avg_long = np.mean(df_1k[PxDataCol.CLOSE].tail(long_period))

    if last_close > avg_short > avg_long:
        return 1
    elif last_close < avg_short < avg_long:
        return -1

    return 0


def calc_strength(df_1k: DataFrame) -> DataFrame:
    strength = sum(_calc_strength(df_1k, 5 * k_period, 10 * k_period) for k_period in [1, 3, 5])

    strength_series = np.empty(len(df_1k) - 1)
    strength_series.fill(np.nan)
    strength_series = np.append(strength_series, [strength])

    df_1k[PxDataCol.STRENGTH] = strength_series

    return df_1k
