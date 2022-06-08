import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from tcoreapi_mq.model import SymbolBaseType


@dataclass(kw_only=True)
class TouchancePxRequestParams:
    symbol_obj: "SymbolBaseType"
    period_mins: list[int]
    history_range: tuple[datetime, datetime]

    request_epoch_sec: float = field(init=False)

    def __post_init__(self):
        self.request_epoch_sec = time.time()

    @property
    def should_re_request(self) -> bool:
        return time.time() - self.request_epoch_sec > 30

    def reset_request_timeout(self) -> None:
        self.request_epoch_sec = time.time()
