from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from kl_site_common.utils import print_warning
from kl_site_server.enums import PxDataCol
from kl_site_server.model import BarDataDict, PxData, PxDataCommon, PxDataConfig
from tcoreapi_mq.message import HistoryInterval, RealtimeData, calc_market_date


@dataclass(kw_only=True)
class PxDataCacheEntry:
    security: str
    symbol_complete: str
    min_tick: float
    decimals: int
    data: dict[int, BarDataDict]  # Epoch sec / bar data
    interval: HistoryInterval
    interval_sec: int

    latest_market: RealtimeData | None = field(init=False, default=None)
    # Storing instead of using `@property` to save performance on request
    latest_epoch_sec: int | None = field(init=False)

    def __post_init__(self):
        self.latest_epoch_sec = max(self.data.keys()) if self.data else None

    @property
    def is_ready(self) -> bool:
        is_ready = bool(self.data)

        if not is_ready:
            print_warning(
                f"[Server] Px data cache entry of [bold]{self.security}@{self.interval_sec // 60}[/bold] not ready"
            )

        return is_ready

    @property
    def data_last_px(self) -> float:
        if not self.is_ready:
            print_warning(
                f"[Server] Px data cache entry of [bold]{self.security}@{self.interval_sec // 60}[/bold] "
                "not ready - failed to request last px from `data`"
            )

        return self.data[self.latest_epoch_sec][PxDataCol.CLOSE]

    def get_last_n_of_close_px(self, count: int) -> list[float]:
        return [
            data[PxDataCol.CLOSE] for epoch_sec, data
            in sorted(self.data.items(), key=lambda item: item[0])[-count:]
        ]

    def remove_oldest(self):
        # Only remove the oldest if there's >1 data
        if len(self.data) > 1:
            self.data.pop(min(self.data.keys()))

    def update_all(self, bars: Iterable[BarDataDict]):
        # `update_all` might be used for partial update,
        # therefore using this instead of creating the whole dict then overwrites it
        for bar in bars:
            self.data[bar[PxDataCol.EPOCH_SEC]] = bar

        # This method could be called with empty `bars`
        if self.data:
            self.latest_epoch_sec = max(self.data.keys())
        else:
            print_warning(f"[Server] `PxDataCacheEntry.update_all()` called, but `bars` is empty")

    def update_latest_market(self, data: RealtimeData):
        """
        Updates the latest market data. Does NOT update the underlying cached data.

        This should be called before the `is_ready` check.
        """
        if self.security != data.security:
            print_warning(
                f"[Server] `update_latest_market()` called at the wrong place - "
                f"symbol not match ({self.security} / {data.security})"
            )
            return

        self.latest_market = data

    def update_latest(self, current: float) -> str | None:
        """
        Updates the latest price, then return the reason of force-send, if allowed.

        This should be called after the `is_ready` check.
        """
        if not self.data:
            # No data fetched yet - no data to be updated
            return

        bar_current = self.data[self.latest_epoch_sec]

        self.data[self.latest_epoch_sec] = bar_current | {
            PxDataCol.HIGH: max(bar_current[PxDataCol.HIGH], current),
            PxDataCol.LOW: min(bar_current[PxDataCol.LOW], current),
            PxDataCol.CLOSE: current,
        }

        if current > bar_current[PxDataCol.HIGH]:
            return "Breaking high"
        if current < bar_current[PxDataCol.LOW]:
            return "Breaking low"

        return None

    def make_new_bar(self, epoch_sec: float) -> float | None:
        if not self.latest_epoch_sec:
            # Data might not be initialized yet - no last bar to inherit the data
            return None

        epoch_int = int(epoch_sec // self.interval_sec * self.interval_sec)
        epoch_sec_time = epoch_int % 86400
        last_px = self.data_last_px

        self.data[epoch_int] = {
            PxDataCol.OPEN: last_px,
            PxDataCol.HIGH: last_px,
            PxDataCol.LOW: last_px,
            PxDataCol.CLOSE: last_px,
            PxDataCol.EPOCH_SEC: epoch_int,
            PxDataCol.EPOCH_SEC_TIME: epoch_sec_time,
            PxDataCol.DATE_MARKET: calc_market_date(
                datetime.fromtimestamp(epoch_int, tz=timezone.utc),
                epoch_sec_time,
                self.symbol_complete
            ),
            PxDataCol.VOLUME: 0,
        }
        self.latest_epoch_sec = epoch_int
        self.remove_oldest()

        return last_px

    def to_px_data(self, px_data_configs: Iterable[PxDataConfig]) -> list[PxData]:
        if not self.is_ready:
            return []

        px_common = PxDataCommon(
            security=self.security,
            min_tick=self.min_tick,
            decimals=self.decimals,
            latest_market=self.latest_market,
            interval_sec=self.interval_sec,
            last_px=self.latest_market.close if self.latest_market else self.data_last_px
        )

        return [px_common.to_px_data(px_data_config) for px_data_config in px_data_configs]
