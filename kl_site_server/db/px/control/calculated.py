from typing import NamedTuple

import pymongo
from pandas import DataFrame
from pymongo.command_cursor import CommandCursor

from kl_site_common.db import start_mongo_txn
from kl_site_common.utils import print_log, split_chunks
from kl_site_server.enums import PxDataCol
from tcoreapi_mq.model import SymbolBaseType
from ..const import px_data_calc_col


def get_calculated_data_from_db(
    symbol_obj: SymbolBaseType, period_min: int, *,
    count: int | None = None, offset: int | None = None,
) -> CommandCursor:
    print_log(
        f"[DB-Px] Getting [purple]{count or '(?)'}{f' (-{offset})' if offset else ''}[/purple] calculated data of "
        f"[yellow]{symbol_obj.security}@{period_min}[/yellow]"
    )

    aggr_stages = [
        {
            "$match": {
                "s": symbol_obj.symbol_complete,
                "p": period_min
            }
        },
        {
            "$sort": {
                PxDataCol.EPOCH_SEC: pymongo.DESCENDING
            }
        },
        {
            "$limit": (count or 2000) + (offset or 0)
        },
        {
            "$sort": {
                PxDataCol.EPOCH_SEC: pymongo.ASCENDING
            }
        },
    ]

    if offset:
        aggr_stages.append({"$skip": offset})

    return px_data_calc_col.aggregate(aggr_stages)


class StoreCalculatedDataArgs(NamedTuple):
    symbol_obj: SymbolBaseType
    period_min: int
    df: DataFrame
    full: bool


def store_calculated_to_db(args: list[StoreCalculatedDataArgs]):
    if not args:
        return

    all_del_conditions = []
    all_recs_insert = []
    for (symbol_obj, period_min, df, full) in args:
        common_filter = {"s": symbol_obj.symbol_complete, "p": period_min}
        recs = df.to_dict("records") if full else [df.iloc[-1].to_dict()]

        all_del_conditions.extend(
            common_filter | {PxDataCol.EPOCH_SEC: rec[PxDataCol.EPOCH_SEC]}
            for rec in recs
        )
        all_recs_insert.extend(common_filter | rec for rec in recs)

    print_log(
        f"[DB-Px] Storing [purple]{len(all_recs_insert)}[/purple] calculated data of "
        f"[yellow]{' / '.join(sorted({arg.symbol_obj.security for arg in args}))}[/yellow]"
    )

    for (del_conditions, recs_insert) in split_chunks(all_del_conditions, all_recs_insert, chunk_size=1000):
        with start_mongo_txn() as session:
            px_data_calc_col.delete_many({"$or": del_conditions}, session=session)
            px_data_calc_col.insert_many(recs_insert, session=session)
