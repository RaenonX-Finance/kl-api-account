from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_diff_full(df: DataFrame) -> DataFrame:
    df[PxDataCol.DIFF] = df[PxDataCol.CLOSE] - df[PxDataCol.OPEN]

    return df


def calc_diff_last(df: DataFrame) -> DataFrame:
    df.at[df.index[-1], PxDataCol.DIFF] = df.at[df.index[-1], PxDataCol.CLOSE] - df.at[df.index[-1], PxDataCol.OPEN]

    return df
