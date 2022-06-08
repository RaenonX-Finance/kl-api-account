from dataclasses import dataclass

from tcoreapi_mq.model import SymbolBaseType
from ._base import RequestBase


@dataclass(kw_only=True)
class QueryInstrumentRequest(RequestBase):
    session_key: str
    symbol_obj: SymbolBaseType

    def to_message_json(self) -> dict:
        return {
            "Request": "QUERYINSTRUMENTINFO",
            "SessionKey": self.session_key,
            "Symbol": self.symbol_obj.symbol_complete
        }
