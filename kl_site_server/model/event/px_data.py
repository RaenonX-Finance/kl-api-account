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
        products = " / ".join({f"[yellow]{px_data.pool.symbol}[/yellow]" for px_data in self.px_data_list})

        return f"{products} - {self.proc_sec:.3f} s"


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    data: RealtimeData

    @property
    def security(self):
        return self.data.security

    def __str__(self):
        return f"[yellow]{self.security}[/yellow] @ {self.data.last_px:.2f}"
