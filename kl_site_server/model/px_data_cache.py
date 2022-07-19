import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, Iterable

from kl_site_common.const import (
    DATA_PX_UPDATE_BATCH_SEC, DATA_PX_UPDATE_CALC_SEC, DATA_PX_UPDATE_MARKET_SEC, DATA_PX_UPDATE_SEC,
)
from kl_site_common.utils import print_log, print_warning
from kl_site_server.enums import PxDataCol
from tcoreapi_mq.message import HistoryData, RealtimeData
from tcoreapi_mq.model import SymbolBaseType
from .bar_data import BarDataDict, to_bar_data_dict_tcoreapi
from .px_data import PxData, PxDataConfig, PxDataPool


@dataclass(kw_only=True)
class PxDataCacheEntry:
    symbol: str
    symbol_complete: str
    min_tick: float
    data: dict[int, BarDataDict]  # Epoch sec / bar data
    interval_sec: int

    latest_market: RealtimeData | None = field(init=False, default=None)

    @property
    def is_ready(self) -> bool:
        is_ready = bool(self.data)

        if not is_ready:
            print_warning(
                f"[Server] Px data cache entry of [bold]{self.symbol} ({self.interval_sec})[/bold] not ready"
            )

        return is_ready

    def remove_oldest(self):
        # Only remove the oldest if there's >1 data
        if len(self.data) > 1:
            self.data.pop(min(self.data.keys()))

    def update_all(self, bars: Iterable[BarDataDict]):
        # `update_all` might be used for partial update,
        # therefore using this instead of creating the whole dict then overwrites it
        for bar in bars:
            self.data[bar[PxDataCol.EPOCH_SEC]] = bar

    def update_latest_market(self, data: RealtimeData) -> None:
        """
        Updates the latest market data. Does NOT update the underlying cached data.

        This should be called before the `is_ready` check.
        """
        if self.symbol != data.security:
            print_warning(
                f"[Server] `update_latest_market()` called at the wrong place - "
                f"symbol not match ({self.symbol} / {data.security})"
            )
            return

        self.latest_market = data

    def update_latest(self, current: float) -> str | None:
        """
        Updates the latest price, then return the reason of force-send, if allowed.

        This should be called after the `is_ready` check.
        """
        # This may be called before the history data arrrives - therefore `self.data` might be empty
        epoch_latest = max(self.data.keys()) if self.data else 0
        epoch_current = int(time.time() // self.interval_sec * self.interval_sec)

        if epoch_current > epoch_latest or epoch_current not in self.data:
            # Current epoch is greater than the latest epoch - create new
            new_bar: BarDataDict = {
                PxDataCol.OPEN: current,
                PxDataCol.HIGH: current,
                PxDataCol.LOW: current,
                PxDataCol.CLOSE: current,
                PxDataCol.EPOCH_SEC: epoch_current,
                PxDataCol.VOLUME: 0,
            }
            self.data[epoch_current] = new_bar
            self.remove_oldest()
            return "New bar"

        bar_current = self.data[epoch_current]
        self.data[epoch_current] = bar_current | {
            PxDataCol.HIGH: max(bar_current[PxDataCol.HIGH], current),
            PxDataCol.LOW: min(bar_current[PxDataCol.LOW], current),
            PxDataCol.CLOSE: current,
        }

        if current > bar_current[PxDataCol.HIGH]:
            return "Breaking high"
        if current < bar_current[PxDataCol.LOW]:
            return "Breaking low"

        return None

    def to_px_data(self, period_mins: list[int]) -> list[PxData]:
        pool = PxDataPool(
            symbol=self.symbol,
            bars=[self.data[key] for key in sorted(self.data.keys())],
            min_tick=self.min_tick,
            latest_market=self.latest_market,
        )

        return [pool.to_px_data(period_min) for period_min in period_mins]


@dataclass(kw_only=True)
class PxDataCache:
    data_1k: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)
    data_dk: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)

    last_market_sent: float | None = field(init=False, default=None)
    last_complete_sent: float | None = field(init=False, default=None)
    last_complete_update_of_symbol: dict[str, float] = field(init=False, default_factory=dict)

    allow_force_send_once_complete: bool = field(init=False, default=False)

    period_mins: DefaultDict[str, list[int]] = field(init=False, default_factory=lambda: defaultdict(list))
    period_days: DefaultDict[str, list[int]] = field(init=False, default_factory=lambda: defaultdict(list))

    buffer_market_data: dict[str, RealtimeData] = field(init=False, default_factory=dict)  # Security / Data
    security_to_symbol_complete: dict[str, str] = field(init=False, default_factory=dict)

    def init_entry(
        self, symbol_obj: SymbolBaseType, min_tick: float,
        period_mins: list[int], period_days: list[int],
    ) -> None:
        symbol_complete = symbol_obj.symbol_complete

        self.security_to_symbol_complete[symbol_obj.symbol] = symbol_complete

        if period_mins:
            self.data_1k[symbol_complete] = PxDataCacheEntry(
                symbol=symbol_obj.symbol,
                symbol_complete=symbol_complete,
                min_tick=min_tick,
                data={},
                interval_sec=60,
            )
            self.period_mins[symbol_complete] = period_mins

        if period_days:
            self.data_dk[symbol_complete] = PxDataCacheEntry(
                symbol=symbol_obj.symbol,
                symbol_complete=symbol_complete,
                min_tick=min_tick,
                data={},
                interval_sec=86400,
            )
            self.period_days[symbol_complete] = period_days

    def _mark_force_send_once(self, symbol_complete: str, reason: str) -> None:
        # Not marking market data to force send
        # because the latest market data will also be updated in the completed one
        print_log(
            f"[Server] Mark [yellow]force send Px[/yellow] allowed once "
            f"for [yellow]{symbol_complete}[/yellow] ({reason})"
        )
        self.allow_force_send_once_complete = True

    def update_complete_data_of_symbol(self, data: HistoryData) -> None:
        symbol_complete = data.symbol_complete

        print_log(
            f"[Server] Updating [purple]{data.data_len_as_str}[/purple] Px data bars "
            f"to [yellow]{symbol_complete}[/yellow] at [yellow]{data.data_type}[/yellow]"
        )
        if data.is_1k:
            if symbol_complete not in self.data_1k:
                print_warning(
                    f"Failed to update complete data of {symbol_complete} @ 1K - "
                    f"data dict not initialized ({list(self.data_1k.keys())})",
                    force=True
                )
                return

            self.data_1k[symbol_complete].update_all(to_bar_data_dict_tcoreapi(bar, 60) for bar in data.data_iter)
            all_data_ready = set(self.data_1k.keys()) == set(self.last_complete_update_of_symbol.keys())
        elif data.is_dk:
            if symbol_complete not in self.data_dk:
                print_warning(
                    f"Failed to update complete data of {symbol_complete} @ DK - "
                    f"data dict not initialized ({list(self.data_1k.keys())})",
                    force=True
                )
                return

            self.data_dk[symbol_complete].update_all(to_bar_data_dict_tcoreapi(bar, 86400) for bar in data.data_iter)
            all_data_ready = set(self.data_dk.keys()) == set(self.last_complete_update_of_symbol.keys())
        else:
            raise ValueError(
                f"No data update as the history data is not either 1K or DK - {symbol_complete} @ {data.data_type}"
            )

        self.last_complete_update_of_symbol[symbol_complete] = time.time()
        if (
                time.time() - min(self.last_complete_update_of_symbol.values()) < DATA_PX_UPDATE_BATCH_SEC
                and all_data_ready
        ):
            self._mark_force_send_once(symbol_complete, "Complete data updated")

    def update_latest_market_data_of_symbol(self, data: RealtimeData) -> None:
        if data_1k := self.data_1k.get(data.symbol_complete):
            data_1k.update_latest_market(data)
        if data_dk := self.data_dk.get(data.symbol_complete):
            data_dk.update_latest_market(data)

    def update_market_data_of_symbol(self, data: RealtimeData) -> None:
        symbol_complete = data.symbol_complete

        reason = None

        if data_1k := self.data_1k.get(symbol_complete):
            reason = data_1k.update_latest(data.last_px) or reason
        if data_dk := self.data_dk.get(symbol_complete):
            reason = data_dk.update_latest(data.last_px) or reason

        if reason:
            self._mark_force_send_once(data.symbol_complete, reason)

    def is_no_market_data_update(self, symbol_complete: str) -> bool:
        # > 3 secs no incoming market data
        return (
                self.last_market_sent is not None
                and time.time() - self.last_market_sent > 3
                and self.is_px_data_ready(symbol_complete)
        )

    def is_send_market_data_ok(self, symbol_complete: str) -> bool:
        if not self.is_px_data_ready(symbol_complete):
            return False

        if self.last_market_sent is None:
            # First market data transmission
            return True

        return time.time() - self.last_market_sent > DATA_PX_UPDATE_MARKET_SEC

    def is_send_complete_data_ok(self, symbol_complete: str) -> bool:
        if self.allow_force_send_once_complete:
            self.allow_force_send_once_complete = False
            return True

        if not self.is_px_data_ready(symbol_complete):
            return False

        if self.last_complete_sent is None:
            # First market data transmission
            return True

        return time.time() - self.last_complete_sent > DATA_PX_UPDATE_SEC

    def rec_buffer_market_data(self, data: RealtimeData):
        self.buffer_market_data[data.security] = data

    def mark_market_data_sent(self) -> None:
        self.last_market_sent = time.time()
        self.buffer_market_data = {}

    def mark_complete_data_sent(self) -> None:
        self.last_complete_sent = time.time() - DATA_PX_UPDATE_CALC_SEC

    def complete_px_data_to_send(self, symbol_complete: str) -> list[PxData]:
        px_data_list = []

        # TODO: Implementation should change after sending specific data to users
        # > Fixing the symbols so `dict size change` error won't pop up if received any history data of new symbol
        symbols_in_data = list(self.data_1k.keys())

        for symbol in symbols_in_data:
            px_cache_entry = self.data_1k[symbol]
            if not px_cache_entry.is_ready:
                print_warning(
                    f"[Server] Complete data of [yellow]{symbol}[/yellow] not ready, "
                    f"skipped processing"
                )
                continue

            for px_data in px_cache_entry.to_px_data(self.period_mins[symbol_complete]):
                px_data_list.append(px_data)

        return px_data_list

    def is_px_data_ready(self, symbol_complete: str) -> bool:
        if symbol_complete in self.data_1k:
            return self.data_1k[symbol_complete].is_ready

        if symbol_complete in self.data_dk:
            return self.data_dk[symbol_complete].is_ready

        return False

    def is_all_px_data_ready(self) -> bool:
        symbols = set(self.data_1k.keys()) | set(self.data_dk.keys())

        return all(self.is_px_data_ready(symbol_complete) for symbol_complete in symbols)

    def get_px_data(self, px_data_configs: Iterable[PxDataConfig]) -> list[PxData]:
        lookup_1k = defaultdict(list)
        lookup_dk = defaultdict(list)

        for px_data_config in px_data_configs:
            security = px_data_config.security
            period_min = px_data_config.period_min

            if not (symbol_complete := self.security_to_symbol_complete.get(security)):
                print_warning(
                    f"Attempt to get the uninitialized Px data of [yellow]{security}[/yellow] @ {period_min}"
                )
                continue

            if period_min >= 1440:
                lookup_dk[symbol_complete].append(period_min)
            else:
                lookup_1k[symbol_complete].append(period_min)

        px_data_list = []

        for symbol_complete, period_mins in lookup_1k.items():
            px_data_list.extend(self.data_1k[symbol_complete].to_px_data(period_mins))

        for symbol_complete, period_mins in lookup_dk.items():
            px_data_list.extend(self.data_dk[symbol_complete].to_px_data(period_mins))

        return px_data_list
