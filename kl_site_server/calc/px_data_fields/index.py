from pandas import DataFrame, DatetimeIndex, to_datetime

from kl_site_server.enums import PxDataCol


def calc_set_epoch_index(df: DataFrame):
    if not df.size:
        raise ValueError("Empty dataframe while setting epoch index")

    df[PxDataCol.DATE] = to_datetime(df[PxDataCol.EPOCH_SEC], utc=True, unit="s")
    df.set_index(DatetimeIndex(df[PxDataCol.DATE]), inplace=True)
