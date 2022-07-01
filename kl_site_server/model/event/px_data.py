from typing import TYPE_CHECKING
from dataclasses import dataclass

from tcoreapi_mq.message import RealtimeData

if TYPE_CHECKING:
    from kl_site_server.model import PxData


@dataclass(kw_only=True)
class OnPxDataUpdatedEvent:
    px_data_list: list["PxData"]

    proc_sec_list: list[float]

    def __str__(self):
        return " / ".join((
            f"{px_data.pool.symbol} @ {px_data.current_close:.2f} - "
            f"{px_data.latest_time.strftime('%m-%d %H:%M')} ({px_data.period_min}) - "
            f"{proc_sec:.3f} s ({px_data.data_count})"
        ) for px_data, proc_sec in zip(self.px_data_list, self.proc_sec_list))


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    data: dict[str, RealtimeData]  # Security / Data

    def __str__(self):
        return " / ".join(f"{data.security} @ {data.last_px:.2f}" for data in self.data.values())
