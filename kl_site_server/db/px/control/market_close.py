import time

from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from kl_site_common.const import DATA_SOURCES
from kl_site_server.utils import generate_bad_request_exception
from ..const import px_session_col
from ..model import FuturesMarketClosedSessionModel, MarketSessionEntry

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
