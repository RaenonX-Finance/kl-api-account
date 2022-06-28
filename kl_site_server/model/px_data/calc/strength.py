import numpy as np
import numpy.typing as npt
import talib
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def _calc_single_period_strength(df_1k: DataFrame, sma_short: int, sma_long: int) -> npt.NDArray[int]:
    is_bull = np.logical_and(
        df_1k[PxDataCol.CLOSE] > df_1k[PxDataCol.get_sma_col_name(sma_short)],
        df_1k[PxDataCol.get_sma_col_name(sma_short)] > df_1k[PxDataCol.get_sma_col_name(sma_long)]
    )
    is_bear = np.logical_and(
        df_1k[PxDataCol.CLOSE] < df_1k[PxDataCol.get_sma_col_name(sma_short)],
        df_1k[PxDataCol.get_sma_col_name(sma_short)] < df_1k[PxDataCol.get_sma_col_name(sma_long)]
    )

    return np.select([is_bull, is_bear], [1, -1])


def calc_strength(df_1k: DataFrame) -> DataFrame:
    sma_periods = [
        5,  # 1K 5T
        10,  # 1K 10T
        15,  # 3K 5T
        30,  # 3K 10T
        25,  # 5K 5T
        50,  # 5K 10T
    ]

    for sma_period in sma_periods:
        df_1k[PxDataCol.get_sma_col_name(sma_period)] = talib.SMA(df_1k[PxDataCol.CLOSE], timeperiod=sma_period)

    strength_series = []

    for k_period in [1, 3, 5]:
        strength_series.append(_calc_single_period_strength(df_1k, 5 * k_period, 10 * k_period))

    df_1k[PxDataCol.STRENGTH] = np.sum(strength_series)

    return df_1k
