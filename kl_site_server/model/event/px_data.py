from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kl_site_server.model import MarketPxUpdateResult


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    result: "MarketPxUpdateResult"

    @property
    def securities(self) -> list[str]:
        return list(self.result.data.keys())

    def __str__(self):
        return " / ".join(
            f"[yellow]{data.security}[/yellow] @ {data.last_px:.2f}"
            for data in self.result.data.values()
        )
