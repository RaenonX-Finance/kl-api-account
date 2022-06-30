from datetime import timedelta
from typing import Callable

import numpy as np
from pandas import DataFrame, DatetimeIndex, to_datetime

from kl_site_server.enums import PxDataCol


def _calc_market_date_nq_ym(df_1k: DataFrame):
    df_1k[PxDataCol.DATE_MARKET] = to_datetime(np.where(
        df_1k[PxDataCol.DATE].dt.hour < 22,
        df_1k[PxDataCol.DATE].dt.date,
        df_1k[PxDataCol.DATE].dt.date + timedelta(days=1)
    ))


def _calc_market_date_fitx(df_1k: DataFrame):
    df_1k[PxDataCol.DATE_MARKET] = to_datetime(np.where(
        (df_1k[PxDataCol.DATE].dt.hour <= 0) & df_1k[PxDataCol.DATE].dt.minute < 45,
        df_1k[PxDataCol.DATE].dt.date,
        df_1k[PxDataCol.DATE].dt.date + timedelta(days=1)
    ))


_calc_function_map: dict[str, Callable[[DataFrame], None]] = {
    "NQ": _calc_market_date_nq_ym,
    "YM": _calc_market_date_nq_ym,
    "FITX": _calc_market_date_fitx,
}


def calc_market_date(df_1k: DataFrame, symbol: str) -> DataFrame:
    df_1k[PxDataCol.DATE] = to_datetime(df_1k[PxDataCol.EPOCH_SEC], utc=True, unit="s")
    df_1k.set_index(DatetimeIndex(df_1k[PxDataCol.DATE]), inplace=True)

    if calc_market_date_symbol := _calc_function_map.get(symbol):
        calc_market_date_symbol(df_1k)
    else:
        raise ValueError(f"Symbol `{symbol}` does not have market date calculation logic")

    return df_1k
