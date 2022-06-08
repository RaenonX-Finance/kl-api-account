import json
from typing import TypedDict


class PxDataMarket(TypedDict):
    symbol: str
    px: float


def to_socket_message_px_data_market(symbol: str, px: float) -> str:
    data: PxDataMarket = {
        "symbol": symbol,
        "px": px,
    }

    return json.dumps(data)
