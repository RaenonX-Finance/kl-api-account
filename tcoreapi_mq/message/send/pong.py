from dataclasses import dataclass

from ._base import RequestBase


@dataclass(kw_only=True)
class PongRequst(RequestBase):
    session_key: str
    id_: str

    def to_message_json(self) -> dict:
        return {
            "Request": "PONG",
            "SessionKey": self.session_key,
            "ID": self.id_
        }
