from datetime import datetime
from typing import Callable

import pymongo
from pymongo.cursor import Cursor

from kl_site_common.db import start_mongo_txn
from kl_site_common.utils import print_log, split_chunks
from tcoreapi_mq.message import HistoryData, HistoryInterval, PxHistoryDataEntry
from ..const import px_data_col
from ..model import DbHistoryDataResult


def _get_history_data_result(fn_get_find_cursor: Callable[[], Cursor]) -> DbHistoryDataResult:
    try:
        return DbHistoryDataResult(
            earliest=fn_get_find_cursor().sort("ts", pymongo.ASCENDING).limit(1).next()["ts"],
            latest=fn_get_find_cursor().sort("ts", pymongo.DESCENDING).limit(1).next()["ts"],
            data=[
                PxHistoryDataEntry.from_mongo_doc(data) for data
                in fn_get_find_cursor().sort("ts", pymongo.ASCENDING)
            ],
        )
    except StopIteration:
        return DbHistoryDataResult(earliest=None, latest=None, data=[])


def get_history_data_from_db_timeframe(
    symbol_complete: str,
    interval: HistoryInterval,
    start: datetime,
    end: datetime,
) -> DbHistoryDataResult:
    print_log(
        f"Requesting history data of [yellow]{symbol_complete}[/] at [yellow]{interval}[/] "
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

    return _get_history_data_result(get_find_cursor)


def get_history_data_from_db_limit_count(
    symbol_complete: str,
    interval: HistoryInterval,
    count: int,
) -> DbHistoryDataResult:
    print_log(
        f"Requesting history data of [yellow]{symbol_complete}[/] at [yellow]{interval}[/] "
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
        f"Requesting history data of [yellow]{symbol_complete}[/] at [yellow]{interval}[/] "
        "- All bars"
    )

    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": interval
        })

    return _get_history_data_result(get_find_cursor)


def get_history_data_close_px_from_db(
    symbol_complete: str,
    count: int,
) -> list[float]:
    return [entry.close for entry in get_history_data_from_db_limit_count(symbol_complete, "1K", count).data]


def get_history_data_at_time_from_db(
    symbol_complete: str,
    epoch_time_secs: list[int], *,
    count: int,
) -> DbHistoryDataResult:
    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": "1K",
            "et": {"$in": epoch_time_secs}
        }).sort("ts", pymongo.DESCENDING).limit(count)

    return _get_history_data_result(get_find_cursor)


def store_history_to_db(data: HistoryData):
    print_log(
        f"Storing [purple]{data.data_len_as_str}[/] history data of "
        f"[yellow]{data.symbol_complete}[/] at [yellow]{data.data_type}[/]"
    )

    for chunk in split_chunks(list(data.to_db_entries()), chunk_size=1000):
        with start_mongo_txn() as session:
            px_data_col.delete_many(
                {"$or": [{"ts": entry["ts"], "s": entry["s"], "i": entry["i"]} for entry in chunk]},
                session=session
            )
            px_data_col.insert_many(chunk, session=session)


def store_history_to_db_from_entries(entries: list[PxHistoryDataEntry]):
    print_log(f"Storing [purple]{len(entries)}[/] history data entries")

    for chunk in split_chunks([entry.to_mongo_doc() for entry in entries], chunk_size=1000):
        with start_mongo_txn() as session:
            px_data_col.delete_many(
                {"$or": [{"ts": entry["ts"], "s": entry["s"], "i": entry["i"]} for entry in chunk]},
                session=session
            )
            px_data_col.insert_many(chunk, session=session)
