import json
from dataclasses import InitVar, dataclass, field
from datetime import datetime

from ..send import HistoryInterval


@dataclass(kw_only=True)
class SubscribePxHistoryMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"


@dataclass(kw_only=True)
class GetPxHistoryDataEntry:
    body: InitVar[dict[str, str]]

    timestamp: datetime = field(init=False)
    epoch_sec: float = field(init=False)

    tick_up: int = field(init=False)
    tick_down: int = field(init=False)
    vol_up: int = field(init=False)
    vol_down: int = field(init=False)
    vol_unch: int = field(init=False)

    open_: int = field(init=False)
    high: int = field(init=False)
    low: int = field(init=False)
    close: int = field(init=False)
    volume: int = field(init=False)

    query_idx: int = field(init=False)

    def __post_init__(self, body: dict[str, str]):
        self.timestamp = datetime.strptime(f"{body['Date']} {body['Time']:>06}", "%Y%m%d %H%M%S")
        self.epoch_sec = self.timestamp.timestamp()

        self.tick_up = int(body["UpTick"])
        self.tick_down = int(body["DownTick"])
        self.vol_up = int(body["UpVolume"])
        self.vol_down = int(body["DownVolume"])
        self.vol_unch = int(body["UnchVolume"])

        self.open_ = int(body["Open"])
        self.high = int(body["High"])
        self.low = int(body["Low"])
        self.close = int(body["Close"])
        self.volume = int(body["Volume"])

        self.query_idx = int(body["QryIndex"])


@dataclass(kw_only=True)
class GetPxHistoryMessage:
    message: InitVar[str]

    symbol_name: str = field(init=False)

    interval: HistoryInterval = field(init=False)
    data: list[GetPxHistoryDataEntry] = field(init=False)

    def __post_init__(self, message: str):
        symbol, data = message.split(":", 1)

        self.symbol_name = symbol

        body = json.loads(data)

        self.interval = body["DataType"]
        self.data = [GetPxHistoryDataEntry(body=data) for data in body["HisData"]]
