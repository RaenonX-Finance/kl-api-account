import json
from dataclasses import dataclass


@dataclass
class MarketPxSubscriptionMessage:
    token: str | None
    security: str

    @staticmethod
    def from_message(message: str) -> "MarketPxSubscriptionMessage":
        return MarketPxSubscriptionMessage(**json.loads(message))


@dataclass
class RequestPxMessage:
    token: str | None
    identifier: str

    @staticmethod
    def from_message(message: str) -> "RequestPxMessage":
        return RequestPxMessage(**json.loads(message))
