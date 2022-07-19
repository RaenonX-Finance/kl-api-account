from typing import TypedDict


class MarketPxSubscriptionMessage(TypedDict):
    token: str | None
    security: str


class RequestPxMessage(TypedDict):
    token: str | None
    identifier: str
