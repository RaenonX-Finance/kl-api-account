from dataclasses import dataclass

from ._base import RequestBase


@dataclass(kw_only=True)
class LogoutRequest(RequestBase):
    session_key: str

    def to_message_json(self) -> dict:
        return {
            "Request": "LOGOUT",
            "SessionKey": self.session_key
        }
