import talib
from pandas import DataFrame

from kl_site_common.const import EmaPeriodPair, INDICATOR_EMA_PERIODS
from kl_site_server.enums import PxDataCol


def _extract_ema_periods(pair: EmaPeriodPair) -> set[int]:
    return {pair["fast"], pair["slow"]}


def calc_ema(df: DataFrame) -> DataFrame:
    for period in INDICATOR_EMA_PERIODS:
        df[PxDataCol.get_ema_col_name(period)] = talib.EMA(df[PxDataCol.CLOSE], timeperiod=period)

    return df
