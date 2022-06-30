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
            f"{self.px_data.pool.symbol} - "
            f"{self.px_data.current_close:.2f} / "
            f"{self.px_data.latest_time} @ {self.px_data.period_min} / "
            f"{self.proc_sec:.3f} s / "
            f"{self.px_data.data_count}"
        )


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    data: dict[str, RealtimeData]  # Security / Data

    def __str__(self):
        return " / ".join(f"{data.security} @ {data.last_px:.2f}" for data in self.data.values())
