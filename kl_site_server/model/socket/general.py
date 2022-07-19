from typing import TypedDict


class PxInitMessage(TypedDict):
    token: str | None
    identifiers: list[str]
