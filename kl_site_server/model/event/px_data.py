from typing import TYPE_CHECKING
from dataclasses import dataclass

from tcoreapi_mq.message import RealtimeData

if TYPE_CHECKING:
    from kl_site_server.model import PxData


@dataclass(kw_only=True)
class OnPxDataUpdatedEvent:
    px_data_list: list["PxData"]

    proc_sec: float

    def __str__(self):
        data_details = " / ".join((
            f"{px_data.pool.symbol} @ {px_data.current_close:.2f} - "
            f"{px_data.latest_time.strftime('%m-%d %H:%M')} ({px_data.period_min}) - "
            f"#{px_data.data_count}"
        ) for px_data in self.px_data_list)

        return f"{self.proc_sec:.3f} s - {data_details}"


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    data: dict[str, RealtimeData]  # Security / Data

    def __str__(self):
        return " / ".join(f"{data.security} @ {data.last_px:.2f}" for data in self.data.values())
