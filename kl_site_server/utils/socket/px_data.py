import json
from typing import Iterable, TYPE_CHECKING, TypedDict

from kl_site_common.const import SR_STRONG_THRESHOLD
from kl_site_server.enums import PxDataCol
from .px_data_market import PxDataMarket, from_realtime_data
from .utils import df_rows_to_list_of_data

if TYPE_CHECKING:
    from kl_site_server.model import PxData


class PxDataBar(TypedDict):
    epochSec: float
    open: float
    high: float
    low: float
    close: float
    diff: float
    strength: int


class PxDataSupportResistance(TypedDict):
    level: float
    strength: float
    strengthCount: float
    strong: bool


class PxDataContract(TypedDict):
    minTick: float
    symbol: str
    name: str


class PxDataDict(TypedDict):
    uniqueIdentifier: str
    periodSec: int
    contract: PxDataContract
    data: list[PxDataBar]
    supportResistance: list[PxDataSupportResistance]
    latestMarket: PxDataMarket


def _from_px_data_last_bar_to_latest_market(px_data: "PxData") -> PxDataMarket:
    current = px_data.get_current()

    open_val = current[PxDataCol.OPEN]
    change_val = current[PxDataCol.CLOSE] - open_val

    return {
        "symbol": px_data.pool.symbol,
        "open": open_val,
        "high": current[PxDataCol.HIGH],
        "low": current[PxDataCol.LOW],
        "close": current[PxDataCol.CLOSE],
        "changeVal": change_val,
        "changePct": change_val / open_val * 100,
    }


def _from_px_data_bars(px_data: "PxData") -> list[PxDataBar]:
    columns = {
        PxDataCol.EPOCH_SEC: "epochSec",
        PxDataCol.OPEN: "open",
        PxDataCol.HIGH: "high",
        PxDataCol.LOW: "low",
        PxDataCol.CLOSE: "close",
        PxDataCol.DIFF: "diff",
        PxDataCol.STRENGTH: "strength"
    }

    return df_rows_to_list_of_data(px_data.dataframe, columns)


def _from_px_data_support_resistance(px_data: "PxData") -> list[PxDataSupportResistance]:
    ret: list[PxDataSupportResistance] = []

    max_strength = max(px_data.sr_levels_data.levels_data, key=lambda data: data.strength).strength

    for sr_level in px_data.sr_levels_data.levels_data:
        # Convert integral absolute strength (5) to relative strength (5 / 10 = 0.5)
        # FIXME: `max_strength` shouldn't be 0, but it's possible for some reason
        strength = sr_level.strength / max_strength if max_strength else 1

        ret.append({
            "level": sr_level.level,
            "strength": strength,
            "strengthCount": sr_level.strength,
            "strong": strength > SR_STRONG_THRESHOLD
        })

    return ret


def _from_px_data_contract(px_data: "PxData") -> PxDataContract:
    return {
        "symbol": px_data.pool.symbol,
        "name": px_data.pool.symbol_name,
        "minTick": px_data.pool.min_tick,
    }


def _to_px_data_dict(px_data: "PxData") -> PxDataDict:
    return {
        "uniqueIdentifier": px_data.unique_identifier,
        "periodSec": px_data.period_min * 60,
        "contract": _from_px_data_contract(px_data),
        "data": _from_px_data_bars(px_data),
        "supportResistance": _from_px_data_support_resistance(px_data),
        # Sending initial data also calls this method
        # In this case, `latest_market` will be `None` since no market data has received yet
        "latestMarket": (
            from_realtime_data(px_data.pool.latest_market)
            if px_data.pool.latest_market
            else _from_px_data_last_bar_to_latest_market(px_data)
        ),
    }


def to_socket_message_px_data(px_data: "PxData") -> str:
    return json.dumps(_to_px_data_dict(px_data))


def to_socket_message_px_data_list(px_data_list: Iterable["PxData"]) -> str:
    data: list[PxDataDict] = [_to_px_data_dict(px_data) for px_data in px_data_list if px_data]

    return json.dumps(data)
