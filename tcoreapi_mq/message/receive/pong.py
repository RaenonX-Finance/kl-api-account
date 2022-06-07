import json
from dataclasses import InitVar, dataclass, field


@dataclass(kw_only=True)
class PongMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
