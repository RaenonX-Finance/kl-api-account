import json
from abc import abstractmethod
from dataclasses import dataclass


@dataclass(kw_only=True)
class RequestBase:
    @abstractmethod
    def to_message_json(self) -> dict:
        raise NotImplementedError()

    def to_message(self) -> str:
        return json.dumps(self.to_message_json())
