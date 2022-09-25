import numpy as np
import talib
from pandas import DataFrame

from kl_site_server.enums import PxDataCol
from kl_site_server.utils import CANDLESTICK_DIR_MACD_SLOW


def calc_candlestick(df: DataFrame) -> DataFrame:
    macd, signal, hist = talib.MACD(
        df[PxDataCol.CLOSE],
        fastperiod=20,
        slowperiod=CANDLESTICK_DIR_MACD_SLOW,
        signalperiod=15
    )

    df[PxDataCol.CANDLESTICK_DIR] = np.where(
        np.isnan(hist),
        0,
        np.where(hist > 0, 1, -1)
    )

    return df
