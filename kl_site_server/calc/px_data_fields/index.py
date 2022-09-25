from pandas import DataFrame, DatetimeIndex, to_datetime

from kl_site_server.enums import PxDataCol


def calc_set_epoch_index(df_1k: DataFrame):
    df_1k[PxDataCol.DATE] = to_datetime(df_1k[PxDataCol.EPOCH_SEC], utc=True, unit="s")
    df_1k.set_index(DatetimeIndex(df_1k[PxDataCol.DATE]), inplace=True)
