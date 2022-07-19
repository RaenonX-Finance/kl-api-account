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
from datetime import datetime, timedelta, timezone
from typing import TypedDict

from ..send import HistoryInterval


def interval_to_timedelta_offset(interval: HistoryInterval) -> timedelta:
    match interval:
        case "1K":
            return timedelta(minutes=1)
        case "DK":
            return timedelta(days=1)
        case _:
            raise ValueError(f"Unable to get `timedelta` from interval `{interval}`")


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


@dataclass(kw_only=True)
class PxHistoryDataEntry:
    timestamp: datetime

    open: float
    high: float
    low: float
    close: float
    volume: int

    symbol_complete: str
    interval: HistoryInterval

    epoch_sec: float = field(init=False)

    def __post_init__(self):
        self.epoch_sec = self.timestamp.timestamp()

    @staticmethod
    def is_valid(body: dict[str, str]) -> bool:
        # Note that `0` here is `str` not numertic type
        return body["Date"] != "0" and body["Time"] != "0"

    @staticmethod
    def from_touchance(
        body: dict[str, str], symbol_complete: str, interval: HistoryInterval
    ) -> "PxHistoryDataEntry":
        ts = datetime.strptime(f"{body['Date']} {body['Time']:>06}", "%Y%m%d %H%M%S").replace(tzinfo=timezone.utc)

        return PxHistoryDataEntry(
            timestamp=ts - interval_to_timedelta_offset(interval),
            open=float(body["Open"]),
            high=float(body["High"]),
            low=float(body["Low"]),
            close=float(body["Close"]),
            volume=int(body["Volume"]),
            symbol_complete=symbol_complete,
            interval=interval,
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
        self.last_query_idx = body["HisData"][-1]["QryIndex"] if body["HisData"] else None

        self.data = {}
        for data in body["HisData"]:
            if not PxHistoryDataEntry.is_valid(data):
                continue

            entry = PxHistoryDataEntry.from_touchance(data, self.symbol_complete, self.interval)
            self.data[entry.timestamp] = entry


@dataclass(kw_only=True)
class CompletePxHistoryMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
