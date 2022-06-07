import json
from dataclasses import InitVar, dataclass, field


@dataclass(kw_only=True)
class SubscribeRealtimeMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"


@dataclass(kw_only=True)
class UnsubscribeRealtimeMessage:
    message: InitVar[str]

    success: bool = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
