import pandas as pd
import talib
from pandas import DataFrame

from kl_site_common.const import EmaPeriodPair, INDICATOR_EMA_PERIODS
from kl_site_server.enums import PxDataCol

# Storing EMA data count because the data count of the original data source
# could change by requesting older data, therefore modifying all EMA calculation results
_EMA_DATA_COUNT: dict[int, int] = {}


def _get_ema_data_count(df: DataFrame, interval_sec: int) -> int:
    if data_count := _EMA_DATA_COUNT.get(interval_sec):
        return data_count

    _EMA_DATA_COUNT[interval_sec] = len(df)

    return _EMA_DATA_COUNT[interval_sec]


def _extract_ema_periods(pair: EmaPeriodPair) -> set[int]:
    return {pair["fast"], pair["slow"]}


def calc_ema(df_source: DataFrame, df_result: DataFrame, interval_sec: int) -> DataFrame:
    df_ema = DataFrame(index=df_result.index)
    df_source = df_source.tail(_get_ema_data_count(df_source, interval_sec))

    for period in INDICATOR_EMA_PERIODS:
        ema_col_name = PxDataCol.get_ema_col_name(period)

        df_ema[ema_col_name] = talib.EMA(df_source[PxDataCol.CLOSE], timeperiod=period)[-len(df_result):]

    return pd.merge(df_result, df_ema, left_index=True, right_index=True)
