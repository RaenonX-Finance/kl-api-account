import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import DefaultDict, Iterable

from kl_site_common.const import DATA_PX_UPDATE_MARKET_SEC, DATA_PX_UPDATE_SEC
from kl_site_common.utils import print_log, print_warning
from kl_site_server.enums import PxDataCol
from tcoreapi_mq.message import HistoryData, RealtimeData
from tcoreapi_mq.model import SymbolBaseType

from .bar_data import BarDataDict, to_bar_data_dict_tcoreapi, to_bar_data_dict_merged
from .px_data import PxData


@dataclass(kw_only=True)
class PxDataCacheEntry:
    symbol: str
    min_tick: float
    period_min: int
    data: dict[int, BarDataDict]  # Epoch sec / bar data

    @property
    def current_epoch_sec(self) -> int:
        if self.period_min >= 1440:
            today = date.today()

            return int(datetime(today.year, today.month, today.day).timestamp())

        period_sec = self.period_min * 60

        return int(time.time() // period_sec * period_sec)

    @property
    def is_ready(self) -> bool:
        is_ready = bool(self.data)

        if not is_ready:
            print_warning(f"[Server] Px data cache entry of [bold]{self.symbol} @ {self.period_min}[/bold] not ready")

        return is_ready

    def remove_oldest(self):
        self.data.pop(min(self.data.keys()))

    def update_all(self, bars_raw: Iterable[BarDataDict]):
        # Different bar may share the same epoch sec because of the interval merger.
        # Therefore, bars are collected according to their epoch sec first, then get the merged bar.
        #   For example, 5 mins contain 1 min x 5
        bar_collector = defaultdict(list)

        for bar in bars_raw:
            bar_collector[bar[PxDataCol.EPOCH_SEC]].append(bar)

        self.data = {
            epoch_sec: to_bar_data_dict_merged(bars)
            for epoch_sec, bars in bar_collector.items()
        }

    def update_latest(self, current: float) -> bool:
        """Returns if force send should be allowed."""
        epoch_latest = max(self.data.keys())
        epoch_current = self.current_epoch_sec

        if epoch_current > epoch_latest:
            # Current epoch is greater than the latest epoch
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
            return True

        bar_current = self.data[epoch_current]
        self.data[epoch_current] = bar_current | {
            PxDataCol.HIGH: max(bar_current[PxDataCol.HIGH], current),
            PxDataCol.LOW: min(bar_current[PxDataCol.LOW], current),
            PxDataCol.CLOSE: current,
        }

        return current > bar_current[PxDataCol.HIGH] or current < bar_current[PxDataCol.LOW]

    def to_px_data(self) -> PxData:
        return PxData(
            symbol_original=self.symbol,
            bars=[self.data[key] for key in sorted(self.data.keys())],
            min_tick=self.min_tick,
            period_min=self.period_min,
        )


@dataclass(kw_only=True)
class PxDataCache:
    data: DefaultDict[str, dict[int, PxDataCacheEntry]] = field(init=False)

    last_market_update: DefaultDict[str, float | None] = field(init=False)  # None means not sent yet
    last_complete_update: DefaultDict[str, float | None] = field(init=False)  # None means not sent yet

    allow_force_send_once_market: DefaultDict[str, bool] = field(init=False)
    allow_force_send_once_complete: DefaultDict[str, bool] = field(init=False)

    def __post_init__(self):
        self.data = defaultdict(dict)

        self.last_market_update = defaultdict(lambda: None)
        self.last_complete_update = defaultdict(lambda: None)
        self.allow_force_send_once_market = defaultdict(lambda: False)
        self.allow_force_send_once_complete = defaultdict(lambda: False)

    @property
    def px_cache_entries(self) -> Iterable[PxDataCacheEntry]:
        return iter(
            px_cache_entry
            for px_cache_entries_of_symbol in self.data.values()
            for px_cache_entry in px_cache_entries_of_symbol.values()
        )

    def init_entry(self, symbol_obj: SymbolBaseType, min_tick: float, period_min: int) -> None:
        self.data[symbol_obj.symbol_complete][period_min] = PxDataCacheEntry(
            symbol=symbol_obj.symbol,
            period_min=period_min,
            min_tick=min_tick,
            data={},
        )

    def get_entries_of_symbol(self, symbol_complete: str) -> Iterable[PxDataCacheEntry]:
        return self.data[symbol_complete].values()

    def _mark_all_force_send_once(self, symbol_complete: str) -> None:
        self.allow_force_send_once_market[symbol_complete] = True
        self.allow_force_send_once_complete[symbol_complete] = True

    def update_complete_data_of_symbol(self, data: HistoryData) -> None:
        symbol_complete = data.handshake.symbol_complete

        for period_min, px_data_entry in self.data[symbol_complete].items():
            print_log(
                f"[Server] Updating {len(data.data_list)} Px data bars to "
                f"[yellow]{symbol_complete} @ {period_min}[/yellow]"
            )
            px_data_entry.update_all(to_bar_data_dict_tcoreapi(bar, period_min) for bar in data.data_list)

        self._mark_all_force_send_once(symbol_complete)

    def update_market_data_of_symbol(self, data: RealtimeData) -> None:
        symbol_complete = data.symbol_complete

        mark_all_force_send = False

        for px_data_entry in self.data[symbol_complete].values():
            mark_all_force_send = px_data_entry.update_latest(data.last_px) or mark_all_force_send

        if mark_all_force_send:
            self._mark_all_force_send_once(symbol_complete)

    def is_no_market_data_update(self, symbol_complete: str) -> bool:
        # > 3 secs no incoming market data
        return (
                self.last_market_update is not None
                and time.time() - self.last_market_update[symbol_complete] > 3
                and all(px_cache_entry.is_ready for px_cache_entry in self.data[symbol_complete].values())
        )

    def is_send_market_data_ok(self, symbol_complete: str) -> bool:
        if self.allow_force_send_once_market[symbol_complete]:
            self.allow_force_send_once_market[symbol_complete] = False
            return True

        if not self.is_px_data_ready(symbol_complete):
            return False

        if self.last_market_update[symbol_complete] is None:
            # First market data transmission
            return True

        return time.time() - self.last_market_update[symbol_complete] > DATA_PX_UPDATE_MARKET_SEC

    def is_send_complete_data_ok(self, symbol_complete: str) -> bool:
        if self.allow_force_send_once_complete[symbol_complete]:
            self.allow_force_send_once_complete[symbol_complete] = False
            return True

        if not self.is_px_data_ready(symbol_complete):
            return False

        if self.last_complete_update[symbol_complete] is None:
            # First market data transmission
            return True

        return time.time() - self.last_complete_update[symbol_complete] > DATA_PX_UPDATE_SEC

    def mark_market_data_sent(self, symbol_complete: str) -> None:
        self.last_market_update[symbol_complete] = time.time()

    def mark_complete_data_sent(self, symbol_complete: str) -> None:
        self.last_complete_update[symbol_complete] = time.time()

    def complete_px_data_to_send(self, symbol_complete: str) -> Iterable[tuple[PxData, float]]:
        if not self.is_send_complete_data_ok(symbol_complete):
            return

        for px_data_entry in self.get_entries_of_symbol(symbol_complete):
            _start = time.time()

            px_data = px_data_entry.to_px_data()

            yield px_data, time.time() - _start

    def is_px_data_ready(self, symbol_complete: str) -> bool:
        return all(px_cache_entry.is_ready for px_cache_entry in self.data[symbol_complete].values())

    def is_all_px_data_ready(self) -> bool:
        return all(self.is_px_data_ready(symbol_complete) for symbol_complete in self.data.keys())
