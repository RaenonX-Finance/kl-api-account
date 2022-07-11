from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def calc_epoch_sec_time(df_1k: DataFrame) -> DataFrame:
    df_1k[PxDataCol.EPOCH_SEC_TIME] = df_1k[PxDataCol.EPOCH_SEC] % 86400

    return df_1k
