import time
from datetime import datetime
from typing import Callable

import pymongo
from bson import ObjectId
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError

from kl_site_common.const import DATA_SOURCES
from kl_site_common.db import start_mongo_txn
from kl_site_common.utils import print_log
from kl_site_server.utils import generate_bad_request_exception
from tcoreapi_mq.message import HistoryData, HistoryInterval, PxHistoryDataEntry
from .const import px_data_col, px_session_col
from .model import DbHistoryDataResult, FuturesMarketClosedSessionModel, MarketSessionEntry


def _get_history_data_result(fn_get_find_cursor: Callable[[], Cursor]) -> DbHistoryDataResult:
    try:
        return DbHistoryDataResult(
            earliest=fn_get_find_cursor().sort("ts", pymongo.ASCENDING).limit(1).next()["ts"],
            latest=fn_get_find_cursor().sort("ts", pymongo.DESCENDING).limit(1).next()["ts"],
            data=[PxHistoryDataEntry.from_mongo_doc(data) for data in fn_get_find_cursor()],
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
        f"[DB-Px] Requesting history data of [yellow]{symbol_complete}[/yellow] at [yellow]{interval}[/yellow] "
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
        f"[DB-Px] Requesting history data of [yellow]{symbol_complete}[/yellow] at [yellow]{interval}[/yellow] "
        f"- {count} bars"
    )

    def get_find_cursor():
        return px_data_col.find({
            "s": symbol_complete,
            "i": interval
        }).sort("ts", pymongo.DESCENDING).limit(count)

    return _get_history_data_result(get_find_cursor)


def store_history_to_db(data: HistoryData):
    print_log(
        f"[DB-Px] Storing [purple]{data.data_len_as_str}[/purple] history data of "
        f"[yellow]{data.symbol_complete}[/yellow] at [yellow]{data.data_type}[/yellow]"
    )

    entries = list(data.to_db_entries())
    chunk_size = 10000
    for i in range(0, len(entries), chunk_size):
        chunk = entries[i:i + chunk_size]

        with start_mongo_txn() as session:
            px_data_col.delete_many(
                {"$or": [{"ts": entry["ts"], "s": entry["s"], "i": entry["i"]} for entry in chunk]},
                session=session
            )
            px_data_col.insert_many(chunk, session=session)


# Keep local backup for faster access and db query count reduction
_market_close_last_updated = None
_market_close_cache: dict[str, list[FuturesMarketClosedSessionModel]] = {}
_market_regular_session: dict[str, list[MarketSessionEntry]] = {
    data_source["symbol"]: [
        MarketSessionEntry.from_config(entry)
        for entry in data_source["session"]
    ] for data_source in DATA_SOURCES
}


def _load_market_close_sessions_to_cache(*, force: bool = False):
    global _market_close_last_updated, _market_close_cache

    if not force and _market_close_last_updated and time.time() - _market_close_last_updated < 60:
        return False

    _market_close_cache = {}
    for data in px_session_col.find():
        data = FuturesMarketClosedSessionModel(**data)

        if data.security not in _market_close_cache:
            _market_close_cache[data.security] = []

        _market_close_cache[data.security].append(data)

    _market_close_last_updated = time.time()


def is_market_closed(security: str) -> bool:
    _load_market_close_sessions_to_cache()

    if security in _market_close_cache and any(entry.is_now_closed for entry in _market_close_cache[security]):
        return True

    return not any(session.is_now_open for session in _market_regular_session[security])


def create_new_market_close_session(data: FuturesMarketClosedSessionModel):
    try:
        px_session_col.insert_one(data.dict())
    except DuplicateKeyError as ex:
        raise generate_bad_request_exception("Same session already exists") from ex

    _load_market_close_sessions_to_cache(force=True)


def delete_market_close_session(closed_session_id: ObjectId):
    del_count = px_session_col.delete_one({"_id": closed_session_id}).deleted_count

    if not del_count:
        raise generate_bad_request_exception(f"Deletion failed - no session matches ID {closed_session_id}")

    _load_market_close_sessions_to_cache(force=True)


def get_all_market_close_session() -> list[FuturesMarketClosedSessionModel]:
    _load_market_close_sessions_to_cache()

    return [entry for entries in _market_close_cache.values() for entry in entries]
