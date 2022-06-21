from dataclasses import dataclass

from tcoreapi_mq.model import SymbolBaseType
from ._base import RequestBase


@dataclass(kw_only=True)
class SubscribeRealtimeRequest(RequestBase):
    session_key: str
    symbol: SymbolBaseType

    def to_message_json(self) -> dict:
        return {
            "Request": "SUBQUOTE",
            "SessionKey": self.session_key,
            "Param": {
                "Symbol": self.symbol.symbol_complete,
                "SubDataType": "REALTIME"
            }
        }


@dataclass(kw_only=True)
class UnsubscribeRealtimeRequest(RequestBase):
    session_key: str
    symbol_complete: str

    def to_message_json(self) -> dict:
        return {
            "Request": "UNSUBQUOTE",
            "SessionKey": self.session_key,
            "Param": {
                "Symbol": self.symbol_complete,
                "SubDataType": "REALTIME"
            }
        }
