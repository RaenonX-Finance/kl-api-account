import json
from dataclasses import InitVar, dataclass, field


@dataclass(kw_only=True)
class ErrorMessage:
    message: InitVar[str]

    session_key: str = field(init=False)
    error: str = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.session_key = body["SessionKey"]
        self.error = body["ErrMsg"]
