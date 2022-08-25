from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import numpy as np
import numpy.typing as npt

from kl_site_common.utils import print_warning
from kl_site_server.enums import PxDataCol
from kl_site_server.model import BarDataDict, PxData, PxDataConfig, PxDataPool
from tcoreapi_mq.message import RealtimeData, calc_market_date


@dataclass(kw_only=True)
class PxDataCacheEntry:
    security: str
    symbol_complete: str
    min_tick: float
    decimals: int
    data: dict[int, BarDataDict]  # Epoch sec / bar data
    interval_sec: int

    latest_market: RealtimeData | None = field(init=False, default=None)
    latest_epoch: int | None = field(init=False)

    def __post_init__(self):
        self.latest_epoch = max(self.data.keys()) if self.data else None

    @property
    def is_ready(self) -> bool:
        is_ready = bool(self.data)

        if not is_ready:
            print_warning(
                f"[Server] Px data cache entry of [bold]{self.security} ({self.interval_sec})[/bold] not ready"
            )

        return is_ready

    @property
    def earliest_epoch_sec(self) -> int:
        return min(self.data.keys())

    @property
    def latest_epoch_sec(self) -> int:
        return max(self.data.keys())

    def get_last_n_of_close_px(self, count: int) -> npt.NDArray[float]:
        return np.array([
            data[PxDataCol.CLOSE] for epoch_sec, data
            in sorted(self.data.items(), key=lambda item: item[0])[-count:]
        ])

    def remove_oldest(self):
        # Only remove the oldest if there's >1 data
        if len(self.data) > 1:
            self.data.pop(min(self.data.keys()))

    def update_all(self, bars: Iterable[BarDataDict]):
        # `update_all` might be used for partial update,
        # therefore using this instead of creating the whole dict then overwrites it
        for bar in bars:
            self.data[bar[PxDataCol.EPOCH_SEC]] = bar

        if self.data:
            # This method could be called with empty `bars`
            self.latest_epoch = max(self.data.keys())

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
        if self.latest_epoch not in self.data:
            # No data fetched yet - no data to be updated
            return

        bar_current = self.data[self.latest_epoch]

        self.data[self.latest_epoch] = bar_current | {
            PxDataCol.HIGH: max(bar_current[PxDataCol.HIGH], current),
            PxDataCol.LOW: min(bar_current[PxDataCol.LOW], current),
            PxDataCol.CLOSE: current,
        }

        if current > bar_current[PxDataCol.HIGH]:
            return "Breaking high"
        if current < bar_current[PxDataCol.LOW]:
            return "Breaking low"

        return None

    def make_new_bar(self, epoch_sec: float):
        if not self.latest_epoch:
            # Data might not be initialized yet - no last bar to "inherit" the data
            return

        epoch_int = int(epoch_sec // self.interval_sec * self.interval_sec)
        epoch_sec_time = epoch_int % 86400
        last_bar = self.data[self.latest_epoch]

        self.data[epoch_int] = {
            PxDataCol.OPEN: last_bar[PxDataCol.CLOSE],
            PxDataCol.HIGH: last_bar[PxDataCol.CLOSE],
            PxDataCol.LOW: last_bar[PxDataCol.CLOSE],
            PxDataCol.CLOSE: last_bar[PxDataCol.CLOSE],
            PxDataCol.EPOCH_SEC: epoch_int,
            PxDataCol.EPOCH_SEC_TIME: epoch_sec_time,
            PxDataCol.DATE_MARKET: calc_market_date(
                datetime.fromtimestamp(epoch_int, tz=timezone.utc),
                epoch_sec_time,
                self.symbol_complete
            ),
            PxDataCol.VOLUME: 0,
        }
        self.latest_epoch = epoch_int
        self.remove_oldest()

    def to_px_data(self, px_data_configs: Iterable[PxDataConfig]) -> list[PxData]:
        pool = PxDataPool(
            security=self.security,
            bars=[self.data[key] for key in sorted(self.data.keys())],
            min_tick=self.min_tick,
            decimals=self.decimals,
            latest_market=self.latest_market,
            interval_sec=self.interval_sec,
        )

        with ThreadPoolExecutor() as executor:
            return [
                future.result() for future
                in as_completed(
                    executor.submit(pool.to_px_data, px_data_config)
                    for px_data_config in px_data_configs
                )
            ]
