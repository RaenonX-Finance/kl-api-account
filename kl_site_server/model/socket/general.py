import json
from dataclasses import dataclass


@dataclass
class PxInitMessage:
    token: str | None
    identifiers: list[str]

    @staticmethod
    def from_message(message: str) -> "PxInitMessage":
        return PxInitMessage(**json.loads(message))
