from dataclasses import dataclass
from typing import TypeAlias

import pymongo

from kl_site_common.db import start_mongo_txn
from kl_site_common.utils import print_log
from ..const import px_data_calc_col


@dataclass(kw_only=True)
class GetCalcDataArgs:
    symbol_complete: str
    period_min: int
    count: int | None = None
    offset: int | None = None


DEFAULT_CALCULATED_DATA_COUNT = 2000
DEFAULT_CALCULATED_DATA_OFFSET = 0

CalcDataLookupInternal: TypeAlias = dict[tuple[str, int], list[dict]]


class CalculatedDataLookup:
    def __init__(self, data: CalcDataLookupInternal | None = None):
        self.data: CalcDataLookupInternal = data or {}  # K = (symbol complete, period min); V = list of data

    @staticmethod
    def _make_key(symbol_complete: str, period_min: int) -> tuple[str, int]:
        return symbol_complete, period_min

    def add_data(self, symbol_complete: str, period_min: int, data: list[dict]):
        self.data[self._make_key(symbol_complete, period_min)] = data

    def get_calculated_data(self, symbol_complete: str, period_min: int) -> list[dict] | None:
        return self.data.get(self._make_key(symbol_complete, period_min))


def get_calculated_data_from_db(
    symbol_complete_list: list[str], period_mins: list[int], *,
    count: int | None = None,
    offset: int | None = None,
    count_override: dict[tuple[str, int], int] | None = None,
    offset_override: dict[tuple[str, int], int] | None = None,
) -> CalculatedDataLookup:
    ret = CalculatedDataLookup()

    if not symbol_complete_list:
        print_log("Skipped getting calculated data - empty symbol list")
        return ret
    elif not period_mins:
        print_log("Skipped getting calculated data - empty period min list")
        return ret

    print_log(f"Getting calculated data of [yellow]{sorted(symbol_complete_list)} @ {sorted(period_mins)}[/]")

    max_count = max([*(count_override or {}).values(), count or DEFAULT_CALCULATED_DATA_COUNT])
    max_offset = max([*(offset_override or {}).values(), offset or DEFAULT_CALCULATED_DATA_OFFSET])

    aggr_pipeline = [
        {
            "$match": {
                "s": {"$in": symbol_complete_list},
                "p": {"$in": period_mins}
            }
        },
        {
            "$sort": {"epoch_sec": pymongo.DESCENDING}
        },
        {
            "$limit": max_count + max_offset
        },
        {
            "$group": {
                "_id": {
                    "s": "$s",
                    "p": "$p"
                },
                "data": {"$push": "$$ROOT"}
            }
        }
    ]

    for aggr_result in px_data_calc_col.aggregate(aggr_pipeline):
        aggr_result_key = aggr_result["_id"]
        symbol_complete = aggr_result_key["s"]
        period_min = aggr_result_key["p"]
        key = (symbol_complete, period_min)

        count = (count_override or {}).get(key) or count or DEFAULT_CALCULATED_DATA_COUNT
        offset = (offset_override or {}).get(key) or offset or DEFAULT_CALCULATED_DATA_OFFSET

        ret.add_data(
            symbol_complete,
            period_min,
            list(reversed(aggr_result["data"][offset:offset + count]))  # noqa: E231
        )

    print_log(f"Obtained calculated data of {sorted(ret.data.keys())}")

    return ret


def _update_px_data_calc(del_conditions: dict, recs_insert: list[dict]):
    with start_mongo_txn() as session:
        px_data_calc_col.delete_many({"$or": del_conditions}, session=session)
        px_data_calc_col.insert_many(recs_insert, session=session)
