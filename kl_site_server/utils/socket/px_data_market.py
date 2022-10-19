import json
from collections.abc import Iterable
from typing import TypeAlias, TypedDict, TYPE_CHECKING

from tcoreapi_mq.message import RealtimeData

if TYPE_CHECKING:
    from kl_site_server.model import OnMarketDataReceivedEvent


class PxDataMarketSingle(TypedDict):
    o: float
    h: float
    l: float
    c: float
    diffVal: float
    diffPct: float
    strength: int


PxDataMarket: TypeAlias = dict[str, PxDataMarketSingle]


def from_realtime_data_single(data: RealtimeData, strength: int) -> PxDataMarketSingle:
    return {
        "o": data.open,
        "h": data.high,
        "l": data.low,
        "c": data.last_px,  # DO NOT use `close` because it is a fixed number for FITX
        "diffVal": data.change_val,
        "diffPct": float(f"{data.change_pct:.3f}"),  # Fix change % to 3 decimal places but in numeric type
        "strength": strength
    }


def to_socket_message_px_data_market(
    e: "OnMarketDataReceivedEvent",
    securities_to_include: Iterable[str]
) -> str | None:
    if not securities_to_include:
        return None

    data_dict: PxDataMarket = {
        security: from_realtime_data_single(e.data[security], e.result.strength[security])
        for security in securities_to_include
        if security in e.data
    }

    return json.dumps(data_dict)
