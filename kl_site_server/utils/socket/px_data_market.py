import json
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from kl_site_server.model import OnMarketDataReceivedEvent


class PxDataMarket(TypedDict):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    change_val: float
    change_pct: float


def to_socket_message_px_data_market(e: "OnMarketDataReceivedEvent") -> str:
    data: PxDataMarket = {
        "symbol": e.data.security,
        "open": e.data.open,
        "high": e.data.high,
        "low": e.data.low,
        "close": e.data.close,
        "change_val": e.data.change_val,
        "change_pct": e.data.change_pct,
    }

    return json.dumps(data)
