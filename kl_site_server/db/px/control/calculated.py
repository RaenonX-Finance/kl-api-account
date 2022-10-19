from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from itertools import product
from typing import Iterable, NamedTuple

import pymongo
from pandas import DataFrame
from pymongo.command_cursor import CommandCursor

from kl_site_common.db import start_mongo_txn
from kl_site_common.utils import print_log, split_chunks
from kl_site_server.enums import PxDataCol
from tcoreapi_mq.model import SymbolBaseType
from ..const import px_data_calc_col


@dataclass(kw_only=True)
class GetCalcDataArgs:
    symbol_complete: str
    period_min: int
    count: int | None = None
    offset: int | None = None


def _get_calculated_data_single(args: GetCalcDataArgs) -> CommandCursor:
    aggr_stages = [
        {
            "$match": {
                "s": args.symbol_complete,
                "p": args.period_min
            }
        },
        {
            "$sort": {
                PxDataCol.EPOCH_SEC: pymongo.DESCENDING
            }
        },
        {
            "$limit": (args.count or 2000) + (args.offset or 0)
        },
        {
            "$sort": {
                PxDataCol.EPOCH_SEC: pymongo.ASCENDING
            }
        },
    ]

    if args.offset:
        aggr_stages.append({"$skip": args.offset})

    return px_data_calc_col.aggregate(aggr_stages)


class CalculatedDataLookup:
    def __init__(self):
        self._data: dict[tuple[str, int], list[dict]] = {}

    @staticmethod
    def _make_key(symbol_complete: str, period_min: int) -> tuple[str, int]:
        return symbol_complete, period_min

    def add_data(self, symbol_complete: str, period_min: int, data: list[dict]):
        self._data[self._make_key(symbol_complete, period_min)] = data

    def get_calculated_data(self, symbol_complete: str, period_min: int) -> list[dict] | None:
        return self._data.get(self._make_key(symbol_complete, period_min))


def get_calculated_data_from_db(
    symbol_complete_list: Iterable[str], period_mins: Iterable[int], *,
    count: int | None = None, offset: int | None = None,
    count_override: dict[tuple[str, int], int] | None = None,
    offset_override: dict[tuple[str, int], int] | None = None,
) -> CalculatedDataLookup:
    print_log(
        f"[DB-Px] Getting calculated data of [yellow]{sorted(symbol_complete_list)} @ {sorted(period_mins)}[/yellow]"
    )

    ret = CalculatedDataLookup()

    with ThreadPoolExecutor() as executor:
        future_to_args = {
            executor.submit(
                _get_calculated_data_single,
                GetCalcDataArgs(
                    symbol_complete=symbol_complete, period_min=period_min,
                    count=(count_override or {}).get((symbol_complete, period_min)) or count,
                    offset=(offset_override or {}).get((symbol_complete, period_min)) or offset,
                )
            ): (symbol_complete, period_min)
            for symbol_complete, period_min in product(symbol_complete_list, period_mins)
        }
        for future in as_completed(future_to_args):
            symbol_complete, period_min = future_to_args[future]

            ret.add_data(symbol_complete, period_min, list(future.result()))

    return ret


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
