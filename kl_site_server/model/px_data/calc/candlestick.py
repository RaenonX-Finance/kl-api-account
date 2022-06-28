import numpy as np
import talib
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_candlestick(df: DataFrame) -> DataFrame:
    macd, signal, hist = talib.MACD(df[PxDataCol.CLOSE], fastperiod=20, slowperiod=300, signalperiod=15)

    df[PxDataCol.CANDLESTICK_DIR] = np.where(hist > 0, 1, -1)

    return df
