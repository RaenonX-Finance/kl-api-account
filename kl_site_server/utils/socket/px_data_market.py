import json
from typing import TypedDict

from tcoreapi_mq.message import RealtimeData


class PxDataMarket(TypedDict):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    changeVal: float
    changePct: float


def from_realtime_data(data: RealtimeData) -> PxDataMarket:
    return {
        "symbol": data.security,
        "open": data.open,
        "high": data.high,
        "low": data.low,
        "close": data.close,
        "changeVal": data.change_val,
        "changePct": data.change_pct,
    }


def to_socket_message_px_data_market(data: RealtimeData) -> str:
    return json.dumps(from_realtime_data(data))
