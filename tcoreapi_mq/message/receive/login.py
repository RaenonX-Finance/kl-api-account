import json
from dataclasses import InitVar, dataclass, field


@dataclass(kw_only=True)
class LoginMessage:
    message: InitVar[str]

    success: bool = field(init=False)
    session_key: str = field(init=False)
    sub_port: int = field(init=False)

    def __post_init__(self, message: str):
        body = json.loads(message)

        self.success = body["Success"] == "OK"
        self.session_key = body["SessionKey"]
        self.sub_port = int(body["SubPort"])
