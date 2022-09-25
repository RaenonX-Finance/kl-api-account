from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_diff(df: DataFrame) -> DataFrame:
    df[PxDataCol.DIFF] = df[PxDataCol.CLOSE] - df[PxDataCol.OPEN]

    return df
