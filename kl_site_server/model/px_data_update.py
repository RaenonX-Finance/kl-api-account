from dataclasses import dataclass

from tcoreapi_mq.message import RealtimeData


@dataclass(kw_only=True)
class MarketPxUpdateResult:
    allow_send: bool
    force_send_reason: str | None
    data: dict[str, RealtimeData]  # Security / Data
    strength: dict[str, int]  # Security / Strength

    @property
    def is_force_send(self):
        return self.force_send_reason is not None
