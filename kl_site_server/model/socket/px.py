from typing import TypedDict
from typing_extensions import NotRequired


class MarketPxSubscriptionMessage(TypedDict):
    token: str | None
    identifiers: list[str]


class RequestPxMessageSingle(TypedDict):
    identifier: str
    offset: NotRequired[int]


class RequestPxMessage(TypedDict):
    token: str | None
    requests: list[RequestPxMessageSingle]
