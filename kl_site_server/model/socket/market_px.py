import json
from dataclasses import dataclass


@dataclass
class MarketPxSubscriptionMessage:
    token: str | None
    security: str

    @staticmethod
    def from_message(message: str) -> "MarketPxSubscriptionMessage":
        return MarketPxSubscriptionMessage(**json.loads(message))
