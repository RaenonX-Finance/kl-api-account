from typing import TypedDict


class PxInitMessage(TypedDict):
    token: str | None
    identifiers: list[str]


class PxCheckAuthMessage(TypedDict):
    token: str | None
