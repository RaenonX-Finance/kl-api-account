import json
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timedelta, timezone

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


@dataclass(kw_only=True)
class PxHistoryDataEntry:
    body: InitVar[dict[str, str]]
    interval: InitVar[HistoryInterval]

    timestamp: datetime = field(init=False)
    epoch_sec: float = field(init=False)

    open: float = field(init=False)
    high: float = field(init=False)
    low: float = field(init=False)
    close: float = field(init=False)
    volume: int = field(init=False)

    query_idx: int = field(init=False)

    def __post_init__(self, body: dict[str, str], interval: HistoryInterval):
        self.timestamp = datetime.strptime(
            f"{body['Date']} {body['Time']:>06}", "%Y%m%d %H%M%S"
        ).replace(tzinfo=timezone.utc) - interval_to_timedelta_offset(interval)
        self.epoch_sec = self.timestamp.timestamp()

        self.open = float(body["Open"])
        self.high = float(body["High"])
        self.low = float(body["Low"])
        self.close = float(body["Close"])
        self.volume = int(body["Volume"])

        self.query_idx = int(body["QryIndex"])


@dataclass(kw_only=True)
class GetPxHistoryMessage:
    message: InitVar[str]

    symbol_complete: str = field(init=False)

    interval: HistoryInterval = field(init=False)
    data: list[PxHistoryDataEntry] = field(init=False)

    def __post_init__(self, message: str):
        symbol_complete, data = message.split(":", 1)

        self.symbol_complete = symbol_complete

        body = json.loads(data)

        self.interval = body["DataType"]
        self.data = [PxHistoryDataEntry(body=data, interval=self.interval) for data in body["HisData"]]


@dataclass(kw_only=True)
class CompletePxHistoryMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
