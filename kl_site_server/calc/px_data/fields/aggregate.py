from typing import Callable

from pandas import DataFrame

from kl_site_server.enums import PxDataCol

_AGGREGATE_DICT: dict[str, Callable | str | list[Callable | str] | dict[str, Callable | str]] = {
    PxDataCol.OPEN: "first",
    PxDataCol.HIGH: "max",
    PxDataCol.LOW: "min",
    PxDataCol.CLOSE: "last",
    PxDataCol.VOLUME: "sum",
    PxDataCol.DATE: "first",
    PxDataCol.DATE_MARKET: "first",
    PxDataCol.EPOCH_SEC_TIME: "first",
    PxDataCol.EPOCH_SEC: "first",  # To keep `PxDataCol.EPOCH_SEC` accessible from the indexer
    PxDataCol.STRENGTH: "last",
}

_AGGREGATE_IGNORE_COLUMNS: set[str] = {
    PxDataCol.EPOCH_SEC,  # Group basis, will be included
}


def aggregate_df(df_1k: DataFrame, period_min: int) -> DataFrame:
    df = df_1k.copy()  # Avoid accidental data override

    if period_min == 1:
        # No aggregation needed, return the original dataframe to save processing time
        return df

    period_sec = period_min * 60

    df[PxDataCol.EPOCH_SEC] = (df[PxDataCol.EPOCH_SEC] // period_sec * period_sec).astype(int)

    # Column processing check
    aggregated_columns = set(_AGGREGATE_DICT.keys()) | _AGGREGATE_IGNORE_COLUMNS
    df_1k_columns = set(df_1k.columns)  # `.columns` has overridden `__eq__`
    if aggregated_columns != df_1k_columns:
        raise ValueError(
            f"Not all columns from `df_1k` are processed ({period_min}):\n"
            f"  1K       : {sorted(df_1k_columns)}\n"
            f"  Aggregate: {sorted(aggregated_columns)}"
        )

    return df.groupby([PxDataCol.EPOCH_SEC]).agg(_AGGREGATE_DICT)
