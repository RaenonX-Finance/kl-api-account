from dataclasses import dataclass

from tcoreapi_mq.message import RealtimeData


@dataclass(kw_only=True)
class OnMarketDataReceivedEvent:
    data: RealtimeData

    @property
    def security(self):
        return self.data.security

    def __str__(self):
        return f"[yellow]{self.security}[/yellow] @ {self.data.last_px:.2f}"
