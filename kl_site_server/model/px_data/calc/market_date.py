from datetime import timedelta

import numpy as np
from pandas import DataFrame, DatetimeIndex, to_datetime

from kl_site_server.enums import PxDataCol


def calc_market_date(df_1k: DataFrame) -> DataFrame:
    df_1k[PxDataCol.DATE] = to_datetime(
        df_1k[PxDataCol.EPOCH_SEC], utc=True, unit="s"
    ).dt.tz_convert("America/Chicago").dt.tz_localize(None)
    df_1k.set_index(DatetimeIndex(df_1k[PxDataCol.DATE]), inplace=True)

    df_1k[PxDataCol.DATE_MARKET] = to_datetime(np.where(
        df_1k[PxDataCol.DATE].dt.hour < 17,
        df_1k[PxDataCol.DATE].dt.date,
        df_1k[PxDataCol.DATE].dt.date + timedelta(days=1)
    ))

    return df_1k
