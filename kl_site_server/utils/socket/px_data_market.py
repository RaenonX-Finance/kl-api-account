import json
from typing import TypeAlias, TypedDict

from tcoreapi_mq.message import RealtimeData


class PxDataMarketSingle(TypedDict):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    changeVal: float
    changePct: float


PxDataMarket: TypeAlias = dict[str, PxDataMarketSingle]


def _from_realtime_data_single(data: RealtimeData) -> PxDataMarketSingle:
    return {
        "symbol": data.security,
        "open": data.open,
        "high": data.high,
        "low": data.low,
        "close": data.last_px,  # DO NOT use `close` because it is a fixed number for FITX
        "changeVal": data.change_val,
        "changePct": data.change_pct,
    }


def to_socket_message_px_data_market(data: RealtimeData) -> str:
    return json.dumps(_from_realtime_data_single(data))
