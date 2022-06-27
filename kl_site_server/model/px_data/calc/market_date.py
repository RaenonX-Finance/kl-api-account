from datetime import timedelta

import numpy as np
from pandas import DataFrame, DatetimeIndex, to_datetime

from kl_site_server.enums import PxDataCol


def calc_market_date(df: DataFrame) -> DataFrame:
    df[PxDataCol.DATE] = to_datetime(
        df[PxDataCol.EPOCH_SEC], utc=True, unit="s"
    ).dt.tz_convert("America/Chicago").dt.tz_localize(None)
    df.set_index(DatetimeIndex(df[PxDataCol.DATE]), inplace=True)

    df[PxDataCol.DATE_MARKET] = to_datetime(np.where(
        df[PxDataCol.DATE].dt.hour < 17,
        df[PxDataCol.DATE].dt.date,
        df[PxDataCol.DATE].dt.date + timedelta(days=1)
    ))

    return df
