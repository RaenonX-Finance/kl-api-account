from dataclasses import dataclass

from ._base import RequestBase
from .types import InstrumentType


@dataclass(kw_only=True)
class QueryAllInstrumentRequest(RequestBase):
    session_key: str
    instrument_type: InstrumentType

    def to_message_json(self) -> dict:
        return {
            "Request": "QUERYALLINSTRUMENT",
            "SessionKey": self.session_key,
            "Type": self.instrument_type
        }
