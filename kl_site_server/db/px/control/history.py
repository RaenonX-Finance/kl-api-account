from datetime import datetime
from typing import Callable

import pymongo
from cachetools.func import ttl_cache
from pymongo.cursor import Cursor
from pymongo.errors import OperationFailure

from kl_site_common.db import start_mongo_txn
from kl_site_common.utils import print_log, print_warning, split_chunks
from tcoreapi_mq.message import HistoryData, HistoryInterval, PxHistoryDataEntry
from ..const import px_data_col
from ..model import DbHistoryDataResult


def _get_history_data_result(
    fn_get_find_cursor: Callable[[], Cursor], *,
    use_native_data_sort: bool = False,
    find_details: str | None = None,
) -> DbHistoryDataResult:
    try:
        if use_native_data_sort:
            data = sorted([
                PxHistoryDataEntry.from_mongo_doc(data) for data
                in fn_get_find_cursor()
            ])
        else:
            data = [
                PxHistoryDataEntry.from_mongo_doc(data) for data
                in fn_get_find_cursor().sort("ts", pymongo.ASCENDING)
            ]

        return DbHistoryDataResult(
            earliest=fn_get_find_cursor().sort("ts", pymongo.ASCENDING).limit(1).next()["ts"],
            latest=fn_get_find_cursor().sort("ts", pymongo.DESCENDING).limit(1).next()["ts"],
            data=data,
        )
    except StopIteration:
        print_warning(f"Empty history data result: {find_details}")

        return DbHistoryDataResult(earliest=None, latest=None, data=[])


def get_history_data_from_db_timeframe(
    symbol_complete: str,
    interval: HistoryInterval,
    start: datetime,
    end: datetime,
) -> DbHistoryDataResult:
    print_log(
        f"Querying history data of [yellow]{symbol_complete}[/] at [yellow]{interval}[/] "
        f"starting from {start} to {end}"
    )

    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": interval,
            "ts": {
                "$gte": start,
                "$lte": end,
            }
        })

    return _get_history_data_result(get_find_cursor, find_details=f"{symbol_complete} @ {interval} ({start} ~ {end})")


def get_history_data_from_db_limit_count(
    symbol_complete: str,
    interval: HistoryInterval,
    count: int,
) -> DbHistoryDataResult:
    print_log(
        f"Querying history data of [yellow]{symbol_complete}[/] at [yellow]{interval}[/] "
        f"- {count} bars"
    )

    aggr_cursor = px_data_col.aggregate([
        {
            "$match": {
                "s": symbol_complete,
                "i": interval
            }
        },
        {
            "$sort": {
                "ts": pymongo.DESCENDING
            }
        },
        {
            "$limit": count
        },
        {
            "$sort": {
                "ts": pymongo.ASCENDING
            }
        }
    ])
    data = [PxHistoryDataEntry.from_mongo_doc(entry) for entry in aggr_cursor]

    if not data:
        raise RuntimeError(f"`{symbol_complete}` @ {interval} (limit {count}) does not have any data")

    return DbHistoryDataResult(
        earliest=data[0].timestamp,
        latest=data[-1].timestamp,
        data=data,
    )


def get_history_data_from_db_full(
    symbol_complete: str,
    interval: HistoryInterval,
) -> DbHistoryDataResult:
    print_log(
        f"Querying history data of [yellow]{symbol_complete}[/] at [yellow]{interval}[/] "
        "- All bars"
    )

    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": interval
        })

    return _get_history_data_result(get_find_cursor, find_details=f"{symbol_complete} @ {interval} (All)")


def get_history_data_close_px_from_db(
    symbol_complete: str,
    count: int,
) -> list[float]:
    return [entry.close for entry in get_history_data_from_db_limit_count(symbol_complete, "1K", count).data]


@ttl_cache(ttl=20)  # No need to re-fetch for every SR level calculation
def get_history_data_at_time_from_db(
    symbol_complete: str,
    epoch_time_secs: tuple[int, ...], *,
    count: int,
) -> DbHistoryDataResult:
    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": "1K",
            "et": {"$in": epoch_time_secs}
        }).sort("ts", pymongo.DESCENDING).limit(count)

    return _get_history_data_result(
        get_find_cursor,
        use_native_data_sort=True,
        find_details=f"{symbol_complete} x {count} @ {epoch_time_secs}"
    )


def store_history_to_db(data: HistoryData, limit: int | None):
    print_log(
        f"Storing [purple]{min(data.data_len, limit or float('inf'))}[/] history data of "
        f"[yellow]{data.symbol_complete}[/] at [yellow]{data.data_type}[/]"
    )

    try:
        for chunk in split_chunks(data.to_db_entries(limit), chunk_size=3000):
            with start_mongo_txn() as session:
                px_data_col.delete_many(
                    {"$or": [{"ts": entry["ts"], "s": entry["s"], "i": entry["i"]} for entry in chunk]},
                    session=session
                )
                px_data_col.insert_many(chunk, session=session)
    except OperationFailure:
        # Retry database ops as sometimes it just fail
        store_history_to_db(data, limit)


def store_history_to_db_from_entries(entries: list[PxHistoryDataEntry]):
    print_log(f"Storing [purple]{len(entries)}[/] history data entries")

    for chunk in split_chunks([entry.to_mongo_doc() for entry in entries], chunk_size=3000):
        with start_mongo_txn() as session:
            px_data_col.delete_many(
                {"$or": [{"ts": entry["ts"], "s": entry["s"], "i": entry["i"]} for entry in chunk]},
                session=session
            )
            px_data_col.insert_many(chunk, session=session)
