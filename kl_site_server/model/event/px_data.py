from typing import TYPE_CHECKING
from dataclasses import dataclass

from tcoreapi_mq.message import RealtimeData

if TYPE_CHECKING:
    from kl_site_server.model import PxData


@dataclass(kw_only=True)
class OnPxDataUpdatedEvent:
    px_data: "PxData"

    proc_sec: float

    def __str__(self):
        return (
            f"{self.px_data.pool.symbol} - {self.px_data.current_close:.2f} / {self.px_data.latest_time} "
            f"@ {self.px_data.period_min} / {self.proc_sec:.3f} s"
        )


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    data: RealtimeData

    def __str__(self):
        return f"{self.data.security} - {self.data.last_px:.2f}"
