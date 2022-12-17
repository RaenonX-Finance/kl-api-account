"""
Sample history data entry:
{
  'Date': '20220715',
  'Time': '92100',
  'UpTick': '4',
  'UpVolume': '4',
  'DownTick': '16',
  'DownVolume': '27',
  'UnchVolume': '66561',
  'Open': '14485',
  'High': '14485',
  'Low': '14482',
  'Close': '14482',
  'Volume': '31',
  'OI': '0',
  'QryIndex': '10701'
}
"""
import json
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timezone
from functools import total_ordering
from typing import TYPE_CHECKING, TypedDict

from kl_site_server.enums import PxDataCol
from .calc import calc_interval_to_timedelta_offset, calc_market_date
from ..send import HistoryInterval

if TYPE_CHECKING:
    from kl_site_server.model import BarDataDict


@dataclass(kw_only=True)
class SubscribePxHistoryMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"


class PxHistoryDataMongoModel(TypedDict):
    ts: datetime  # Timestamp
    o: float  # Open
    h: float  # High
    l: float  # Low
    c: float  # Close
    v: int  # Volume
    s: str  # Symbol (complete)
    i: HistoryInterval  # Interval
    e: float  # Epoch sec
    et: float  # Epoch sec - time only
    m: datetime


@total_ordering
@dataclass(kw_only=True)
class PxHistoryDataEntry:
    # Some names are synced with the value of `PxDataCol`
    timestamp: datetime

    open: float
    high: float
    low: float
    close: float
    volume: int

    symbol_complete: str
    interval: HistoryInterval

    epoch_sec: float
    epoch_sec_time: float
    market_date: datetime

    def __eq__(self, other):
        if not isinstance(other, PxHistoryDataEntry):
            return ValueError(f"Cannot compare {type(other)} with {PxHistoryDataEntry}")

        return self.timestamp == other.timestamp

    def __lt__(self, other):
        if not isinstance(other, PxHistoryDataEntry):
            return ValueError(f"Cannot compare {type(other)} with {PxHistoryDataEntry}")

        return self.timestamp < other.timestamp

    @staticmethod
    def is_valid(body: dict[str, str]) -> bool:
        # Note that `0` here is `str` not numeric type
        return body["Date"] != "0"

    @staticmethod
    def _ts_to_epoch(ts: datetime) -> tuple[float, float]:
        epoch_sec = ts.timestamp()
        epoch_sec_time = epoch_sec % 86400

        return epoch_sec, epoch_sec_time

    @classmethod
    def from_touchance(
        cls, body: dict[str, str], symbol_complete: str, interval: HistoryInterval
    ) -> "PxHistoryDataEntry":
        ts = datetime.strptime(f"{body['Date']} {body['Time']:>06}", "%Y%m%d %H%M%S").replace(tzinfo=timezone.utc)
        ts -= calc_interval_to_timedelta_offset(interval)

        epoch_sec, epoch_sec_time = cls._ts_to_epoch(ts)

        return PxHistoryDataEntry(
            timestamp=ts,
            open=float(body["Open"]),
            high=float(body["High"]),
            low=float(body["Low"]),
            close=float(body["Close"]),
            volume=int(body["Volume"]),
            symbol_complete=symbol_complete,
            interval=interval,
            epoch_sec=epoch_sec,
            epoch_sec_time=epoch_sec_time,
            market_date=calc_market_date(ts, epoch_sec_time, symbol_complete),
        )

    @staticmethod
    def from_mongo_doc(doc: PxHistoryDataMongoModel) -> "PxHistoryDataEntry":
        return PxHistoryDataEntry(
            timestamp=doc["ts"],
            open=doc["o"],
            high=doc["h"],
            low=doc["l"],
            close=doc["c"],
            volume=doc["v"],
            symbol_complete=doc["s"],
            interval=doc["i"],
            epoch_sec=doc["e"],
            epoch_sec_time=doc["et"],
            market_date=doc["m"],
        )

    @staticmethod
    def from_bar_data_dict(
        symbol_complete: str, interval: HistoryInterval, bar_data: "BarDataDict"
    ) -> "PxHistoryDataEntry":
        epoch_sec = bar_data[PxDataCol.EPOCH_SEC]

        return PxHistoryDataEntry(
            timestamp=datetime.fromtimestamp(epoch_sec, tz=timezone.utc),
            open=bar_data[PxDataCol.OPEN],
            high=bar_data[PxDataCol.HIGH],
            low=bar_data[PxDataCol.LOW],
            close=bar_data[PxDataCol.CLOSE],
            volume=bar_data[PxDataCol.VOLUME],
            symbol_complete=symbol_complete,
            interval=interval,
            epoch_sec=epoch_sec,
            epoch_sec_time=bar_data[PxDataCol.EPOCH_SEC_TIME],
            market_date=bar_data[PxDataCol.DATE_MARKET],
        )

    @classmethod
    def make_new_bar(
        cls,
        symbol_complete: str,
        interval: HistoryInterval,
        ts: datetime,
        px: float,
    ) -> "PxHistoryDataEntry":
        epoch_sec, epoch_sec_time = cls._ts_to_epoch(ts)

        return PxHistoryDataEntry(
            timestamp=ts,
            open=px,
            high=px,
            low=px,
            close=px,
            volume=0,
            symbol_complete=symbol_complete,
            interval=interval,
            epoch_sec=epoch_sec,
            epoch_sec_time=epoch_sec_time,
            market_date=calc_market_date(ts, epoch_sec_time, symbol_complete),
        )

    def to_mongo_doc(self) -> PxHistoryDataMongoModel:
        return {
            "ts": self.timestamp,
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.volume,
            "s": self.symbol_complete,
            "i": self.interval,
            "e": self.epoch_sec,
            "et": self.epoch_sec_time,
            "m": self.market_date,
        }


@dataclass(kw_only=True)
class GetPxHistoryMessage:
    message: InitVar[str]

    symbol_complete: str = field(init=False)

    interval: HistoryInterval = field(init=False)
    data: dict[datetime, PxHistoryDataEntry] = field(init=False)
    last_query_idx: int | None = field(init=False)

    def __post_init__(self, message: str):
        symbol_complete, data = message.split(":", 1)

        self.symbol_complete = symbol_complete

        body = json.loads(data)

        self.interval = body["DataType"]
        self.last_query_idx = int(body["HisData"][-1]["QryIndex"]) if body["HisData"] else None

        self.data = {}
        for data in body["HisData"]:
            if not PxHistoryDataEntry.is_valid(data):
                continue

            try:
                entry = PxHistoryDataEntry.from_touchance(data, self.symbol_complete, self.interval)
                self.data[entry.timestamp] = entry
            except ValueError:
                # Temporary fix of erroneous datetime format
                # https://github.com/RaenonX-Finance/kl-site-back/issues/80
                continue


@dataclass(kw_only=True)
class CompletePxHistoryMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
