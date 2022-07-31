import json
from typing import TypeAlias, TypedDict

from tcoreapi_mq.message import RealtimeData


class PxDataMarketSingle(TypedDict):
    o: float
    h: float
    l: float
    c: float
    diffVal: float
    diffPct: float


PxDataMarket: TypeAlias = dict[str, PxDataMarketSingle]


def from_realtime_data_single(data: RealtimeData) -> PxDataMarketSingle:
    return {
        "o": data.open,
        "h": data.high,
        "l": data.low,
        "c": data.last_px,  # DO NOT use `close` because it is a fixed number for FITX
        "diffVal": data.change_val,
        "diffPct": float(f"{data.change_pct:.3f}"),  # Fix change % to 3 decimal places but in numeric type
    }


def to_socket_message_px_data_market(data: list[RealtimeData]) -> str:
    data_dict: PxDataMarket = {data.security: from_realtime_data_single(data) for data in data}

    return json.dumps(data_dict)
