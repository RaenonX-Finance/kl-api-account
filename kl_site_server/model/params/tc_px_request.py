import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from kl_site_server.utils import get_start_offset

if TYPE_CHECKING:
    from tcoreapi_mq.model import SymbolBaseType


@dataclass(kw_only=True)
class TouchancePxRequestParams:
    symbol_obj: "SymbolBaseType"
    period_mins: list[int]
    period_days: list[int]
    history_range_1k: tuple[datetime, datetime]
    history_range_dk: tuple[datetime, datetime]

    request_epoch_sec: float = field(init=False)

    def __post_init__(self):
        self.request_epoch_sec = time.time()
        self.history_range_1k = (
            self.history_range_1k[0] - get_start_offset(max(self.period_mins)),
            self.history_range_1k[1],
        )
        self.history_range_dk = (
            self.history_range_dk[0] - get_start_offset(max(self.period_days or [0]) * 1440),
            self.history_range_dk[1],
        )

    @property
    def should_re_request(self) -> bool:
        # 30 secs CD as suggested by the customer support
        return time.time() - self.request_epoch_sec > 30

    def reset_request_timeout(self) -> None:
        self.request_epoch_sec = time.time()
