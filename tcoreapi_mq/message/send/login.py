from dataclasses import dataclass

from ._base import RequestBase


@dataclass(kw_only=True)
class LoginRequest(RequestBase):
    app_id: str
    service_key: str

    def to_message_json(self) -> dict:
        return {
            "Request": "LOGIN",
            "Param": {
                "SystemName": self.app_id,
                "ServiceKey": self.service_key
            }
        }
