from typing import TypedDict


class MarketPxSubscriptionMessage(TypedDict):
    token: str | None
    securities: list[str]


class RequestPxMessageSingle(TypedDict):
    identifier: str
    offset: int | None


class RequestPxMessage(TypedDict):
    token: str | None
    requests: list[RequestPxMessageSingle]
