from typing import TypedDict


class MarketPxSubscriptionMessage(TypedDict):
    token: str | None
    securities: list[str]


class RequestPxMessage(TypedDict):
    token: str | None
    identifiers: list[str]
