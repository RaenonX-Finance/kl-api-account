from dataclasses import dataclass
from datetime import datetime

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

    def to_message_json(self) -> dict:
        return {
            "Request": "SUBQUOTE",
            "SessionKey": self.session_key,
            "Param": {
                "Symbol": self.symbol.symbol_complete,
                "SubDataType": self.interval,
                "StartTime": self.start_time.strftime("%Y%m%d%H"),
                "EndTime": self.end_time.strftime("%Y%m%d%H")
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
class CompletePxHistoryRequest(RequestBase):
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
