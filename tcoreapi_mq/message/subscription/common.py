import json
from typing import Any

from .types import DataType


class CommonData:
    def __init__(self, message: str):
        self.text: str = message
        self.body: dict[str, Any] = json.loads(message)

    @property
    def data_type(self) -> DataType:
        return self.body["DataType"]
