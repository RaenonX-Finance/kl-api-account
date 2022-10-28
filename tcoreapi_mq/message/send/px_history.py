from dataclasses import dataclass, field
from datetime import datetime, timedelta

from tcoreapi_mq.model import SymbolBaseType
from ._base import RequestBase
from .types import HistoryInterval


@dataclass(kw_only=True)
class SubscribePxHistoryRequest(RequestBase):
    session_key: str
    symbol: SymbolBaseType
    interval: HistoryInterval
    start_time: datetime
    end_time: datetime

    start_ts_str: str = field(init=False)
    end_ts_str: str = field(init=False)

    def __post_init__(self):
        self.start_ts_str = self.start_time.strftime("%Y%m%d%H")
        # End time in message is exclusive in range (18~19 only fetches data until 18:59 not 19:59)
        self.end_ts_str = (self.end_time + timedelta(hours=1)).strftime("%Y%m%d%H")

        if self.start_ts_str == self.end_ts_str:
            raise ValueError(
                "Per Touchance customer service's response, `start_time` and `end_time` must be different. "
                f"(Current: {self.start_ts_str})"
            )

    def to_message_json(self) -> dict:
        return {
            "Request": "SUBQUOTE",
            "SessionKey": self.session_key,
            "Param": {
                "Symbol": self.symbol.symbol_complete,
                "SubDataType": self.interval,
                "StartTime": self.start_ts_str,
                "EndTime": self.end_ts_str
            }
        }


@dataclass(kw_only=True)
class GetPxHistoryRequest(RequestBase):
    session_key: str
    symbol_complete: str
    interval: HistoryInterval
    start_time_str: str
    end_time_str: str
    query_idx: int

    def to_message_json(self) -> dict:
        return {
            "Request": "GETHISDATA",
            "SessionKey": self.session_key,
            "Param": {
                "Symbol": self.symbol_complete,
                "SubDataType": self.interval,
                "StartTime": self.start_time_str,
                "EndTime": self.end_time_str,
                "QryIndex": str(self.query_idx)  # This must be in type of `str`
            }
        }


@dataclass(kw_only=True)
class UnsubscribePxHistoryRequest(RequestBase):
    session_key: str
    symbol_complete: str
    interval: HistoryInterval
    start_time_str: str
    end_time_str: str

    def to_message_json(self) -> dict:
        return {
            "Request": "UNSUBQUOTE",
            "SessionKey": self.session_key,
            "Param": {
                "Symbol": self.symbol_complete,
                "SubDataType": self.interval,
                "StartTime": self.start_time_str,
                "EndTime": self.end_time_str,
            }
        }
