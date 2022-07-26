import numpy as np
import pandas as pd
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_tie_point(df_1k: DataFrame, period_min: int) -> DataFrame:
    if period_min >= 1440:  # Period > 1 day, tie point is meaningless
        df_1k[PxDataCol.TIE_POINT] = np.empty(len(df_1k))

        return df_1k

    mkt_date_group = df_1k.groupby(PxDataCol.DATE_MARKET)

    df_1k[PxDataCol.TIE_POINT] = np.add(
        mkt_date_group[PxDataCol.HIGH].transform(pd.Series.cummax),
        mkt_date_group[PxDataCol.LOW].transform(pd.Series.cummin),
    ) / 2

    return df_1k
