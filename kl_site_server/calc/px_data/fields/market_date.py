from typing import Callable

import numpy as np
from pandas import DataFrame, DatetimeIndex, to_datetime
from pandas.tseries.offsets import BDay

from kl_site_common.utils import get_epoch_sec_time
from kl_site_server.enums import PxDataCol


# + BDay(0) because `df_1k[PxDataCol.DATE].dt.date` somehow gives int, causing `to_datetime` error


def _calc_market_date_nq_ym(df_1k: DataFrame):
    df_1k[PxDataCol.DATE_MARKET] = to_datetime(np.where(
        df_1k[PxDataCol.EPOCH_SEC_TIME] < get_epoch_sec_time(22),
        # Don't use `dt.date` as it changes datatype and drags the performance down
        df_1k[PxDataCol.DATE].dt.normalize() + BDay(0),
        df_1k[PxDataCol.DATE].dt.normalize() + BDay(1),
    ))


def _calc_market_date_fitx(df_1k: DataFrame):
    df_1k[PxDataCol.DATE_MARKET] = to_datetime(np.where(
        df_1k[PxDataCol.EPOCH_SEC_TIME] >= get_epoch_sec_time(0, 45),
        # Don't use `dt.date` as it changes datatype and drags the performance down
        df_1k[PxDataCol.DATE].dt.normalize() + BDay(0),
        df_1k[PxDataCol.DATE].dt.normalize() - BDay(1),
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
