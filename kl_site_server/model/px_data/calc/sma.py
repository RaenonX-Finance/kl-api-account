import talib
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_sma(df: DataFrame) -> DataFrame:
    # EXNOTE: SMA periods to use for indicators/strategies - 5 & 10
    for sma_period in [5, 10]:
        df[PxDataCol.get_sma_col_name(sma_period)] = talib.SMA(
            df[PxDataCol.CLOSE],
            timeperiod=sma_period
        )

    return df
