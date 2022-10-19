from typing import Iterable, TYPE_CHECKING, TypeAlias, TypedDict

from kl_site_common.const import (
    EMA_PERIOD_PAIRS_STRONG_SR, EMA_PERIOD_PAIR_NET, EmaPeriodPair, INDICATOR_EMA_PERIODS,
)
from kl_site_common.utils import print_warning
from kl_site_server.enums import PxDataCol
from .px_data_market import PxDataMarketSingle, from_realtime_data_single
from .utils import data_rename_col, dump_and_compress

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
    candlestick: int
    tiePoint: float


class PxDataSupportResistance(TypedDict):
    groups: list[list[float]]
    basic: list[float]


class PxDataContract(TypedDict):
    minTick: float
    decimals: int
    symbol: str
    name: str


PxDataEmaPeriodPair: TypeAlias = EmaPeriodPair


class PxDataEmaConfig(TypedDict):
    net: PxDataEmaPeriodPair
    strongSr: list[PxDataEmaPeriodPair]


class PxDataIndicatorConfig(TypedDict):
    ema: PxDataEmaConfig


class PxDataDict(TypedDict):
    uniqueIdentifier: str
    periodSec: int
    contract: PxDataContract
    data: list[PxDataBar]
    offset: int | None
    strength: int
    supportResistance: PxDataSupportResistance
    latestMarket: PxDataMarketSingle
    indicator: PxDataIndicatorConfig


def _from_px_data_last_bar_to_latest_market(px_data: "PxData") -> PxDataMarketSingle:
    current = px_data.get_current()

    open_val = current[PxDataCol.OPEN]
    change_val = current[PxDataCol.CLOSE] - open_val

    return {
        "o": open_val,
        "h": current[PxDataCol.HIGH],
        "l": current[PxDataCol.LOW],
        "c": current[PxDataCol.CLOSE],
        "diffVal": change_val,
        "diffPct": float(f"{change_val / open_val * 100:.3f}"),
        "strength": px_data.common.strength,
    }


def _from_px_data_bars(px_data: "PxData") -> list[PxDataBar]:
    columns = {
        PxDataCol.EPOCH_SEC: "epochSec",
        PxDataCol.OPEN: "open",
        PxDataCol.HIGH: "high",
        PxDataCol.LOW: "low",
        PxDataCol.CLOSE: "close",
        PxDataCol.DIFF: "diff",
        PxDataCol.CANDLESTICK_DIR: "candlestick",
        PxDataCol.TIE_POINT: "tiePoint"
    }
    columns |= {
        PxDataCol.get_ema_col_name(period): f"ema{period}"
        for period in INDICATOR_EMA_PERIODS
    }

    if not px_data.data:
        print_warning(f"Px data of `{px_data.unique_identifier}` does not contain any data")
        return []

    return data_rename_col(px_data.data, columns)


def _from_px_data_support_resistance(px_data: "PxData") -> PxDataSupportResistance:
    return {
        "groups": px_data.common.sr_levels_data.groups,
        "basic": px_data.common.sr_levels_data.basic
    }


def _from_px_data_contract(px_data: "PxData") -> PxDataContract:
    return {
        "minTick": px_data.common.min_tick,
        "decimals": px_data.common.decimals,
        "symbol": px_data.common.security,
        "name": px_data.common.symbol_name,
    }


def _get_ema_config() -> PxDataEmaConfig:
    return {
        "net": EMA_PERIOD_PAIR_NET,
        "strongSr": EMA_PERIOD_PAIRS_STRONG_SR,
    }


def _get_indicator_config() -> PxDataIndicatorConfig:
    return {
        "ema": _get_ema_config()
    }


def _to_px_data_dict(px_data: "PxData") -> PxDataDict:
    return {
        "uniqueIdentifier": px_data.unique_identifier,
        "periodSec": px_data.period_min * 60,
        "contract": _from_px_data_contract(px_data),
        "data": _from_px_data_bars(px_data),
        "offset": px_data.offset,
        "supportResistance": _from_px_data_support_resistance(px_data),
        "strength": px_data.common.strength,
        # Sending initial data also calls this method
        # In this case, `latest_market` will be `None` since no market data has received yet
        "latestMarket": (
            from_realtime_data_single(px_data.common.latest_market, px_data.common.strength)
            if px_data.common.latest_market
            else _from_px_data_last_bar_to_latest_market(px_data)
        ),
        "indicator": _get_indicator_config()
    }


def to_socket_message_px_data_list(px_data_list: Iterable["PxData"]) -> bytes:
    data: list[PxDataDict] = [_to_px_data_dict(px_data) for px_data in px_data_list if px_data.data]

    return dump_and_compress(data)
