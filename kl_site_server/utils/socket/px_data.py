import json
from typing import Iterable, TYPE_CHECKING, TypedDict

from kl_site_common.const import SMA_PERIODS, SR_STRONG_THRESHOLD
from kl_site_server.enums import PxDataCol
from .utils import df_rows_to_list_of_data

if TYPE_CHECKING:
    from kl_site_server.model import PxData


class PxDataBar(TypedDict):
    epochSec: float
    open: float
    high: float
    low: float
    close: float
    vwap: float
    diff: float


class PxDataSupportResistance(TypedDict):
    level: float
    strength: float
    strengthCount: float
    strong: bool


class PxDataContract(TypedDict):
    minTick: float
    symbol: str


class PxDataLastDayDiff(TypedDict):
    px: float
    percent: float


class PxDataDict(TypedDict):
    uniqueIdentifier: str
    periodSec: int
    contract: PxDataContract
    data: list[PxDataBar]
    supportResistance: list[PxDataSupportResistance]
    lastDayClose: float | None
    todayOpen: float | None
    smaPeriods: list[int]


def _from_px_data_bars(px_data: "PxData") -> list[PxDataBar]:
    columns = {
        PxDataCol.EPOCH_SEC: "epochSec",
        PxDataCol.OPEN: "open",
        PxDataCol.HIGH: "high",
        PxDataCol.LOW: "low",
        PxDataCol.CLOSE: "close",
        PxDataCol.VWAP: "vwap",
        PxDataCol.DIFF: "diff",
    }
    columns |= {
        PxDataCol.get_sma_col_name(sma_period): f"sma{sma_period}"
        for sma_period in SMA_PERIODS
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
        "symbol": px_data.symbol,
        "minTick": px_data.min_tick,
    }


def _to_px_data_dict(px_data: "PxData") -> PxDataDict:
    return {
        "uniqueIdentifier": px_data.unique_identifier,
        "periodSec": px_data.period_sec,
        "contract": _from_px_data_contract(px_data),
        "data": _from_px_data_bars(px_data),
        "supportResistance": _from_px_data_support_resistance(px_data),
        "lastDayClose": px_data.get_last_day_close(),
        "todayOpen": px_data.get_today_open(),
        "smaPeriods": SMA_PERIODS,
    }


def to_socket_message_px_data(px_data: "PxData") -> str:
    return json.dumps(_to_px_data_dict(px_data))


def to_socket_message_px_data_list(px_data_list: Iterable["PxData"]) -> str:
    data: list[PxDataDict] = [_to_px_data_dict(px_data) for px_data in px_data_list if px_data]

    return json.dumps(data)
