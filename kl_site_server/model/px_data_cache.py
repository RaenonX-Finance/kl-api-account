import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import DefaultDict, Iterable

from kl_site_common.const import MARKET_PX_TIME_GATE_SEC
from kl_site_common.utils import print_log, print_warning
from kl_site_server.enums import PxDataCol
from tcoreapi_mq.message import HistoryData, RealtimeData, SystemTimeData, calc_market_date
from tcoreapi_mq.model import SymbolBaseType
from .bar_data import BarDataDict, to_bar_data_dict_tcoreapi
from .px_data import PxData, PxDataConfig, PxDataPool
from .px_data_update import MarketPxUpdateResult


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
        last_bar = self.data[self.latest_epoch]

        self.data[epoch_int] = {
            PxDataCol.OPEN: last_bar[PxDataCol.CLOSE],
            PxDataCol.HIGH: last_bar[PxDataCol.CLOSE],
            PxDataCol.LOW: last_bar[PxDataCol.CLOSE],
            PxDataCol.CLOSE: last_bar[PxDataCol.CLOSE],
            PxDataCol.EPOCH_SEC: epoch_int,
            PxDataCol.EPOCH_SEC_TIME: epoch_int % 86400,
            PxDataCol.DATE_MARKET: calc_market_date(
                datetime.fromtimestamp(epoch_int, tz=timezone.utc),
                epoch_int,
                self.symbol_complete
            ),
            PxDataCol.VOLUME: 0,
        }
        self.latest_epoch = epoch_int
        self.remove_oldest()

    def to_px_data(self, px_data_configs: Iterable[PxDataConfig]) -> list[PxData]:
        pool = PxDataPool(
            symbol=self.security,
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


@dataclass(kw_only=True)
class PxDataCache:
    data_1k: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)
    data_dk: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)

    last_market_send: float = field(init=False, default=0)

    period_mins: DefaultDict[str, list[int]] = field(init=False, default_factory=lambda: defaultdict(list))
    period_days: DefaultDict[str, list[int]] = field(init=False, default_factory=lambda: defaultdict(list))

    security_to_symbol_complete: dict[str, str] = field(init=False, default_factory=dict)

    buffer_mkt_px: dict[str, RealtimeData] = field(init=False, default_factory=dict)  # Security / Data

    def init_entry(
        self, *, symbol_obj: SymbolBaseType, min_tick: float, decimals: int,
        period_mins: list[int], period_days: list[int],
    ) -> None:
        symbol_complete = symbol_obj.symbol_complete

        self.security_to_symbol_complete[symbol_obj.security] = symbol_complete

        if period_mins:
            self.data_1k[symbol_complete] = PxDataCacheEntry(
                security=symbol_obj.security,
                symbol_complete=symbol_complete,
                min_tick=min_tick,
                decimals=decimals,
                data={},
                interval_sec=60,
            )
            self.period_mins[symbol_complete] = period_mins

        if period_days:
            self.data_dk[symbol_complete] = PxDataCacheEntry(
                security=symbol_obj.security,
                symbol_complete=symbol_complete,
                min_tick=min_tick,
                decimals=decimals,
                data={},
                interval_sec=86400,
            )
            self.period_days[symbol_complete] = period_days

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

            self.data_1k[symbol_complete].update_all(to_bar_data_dict_tcoreapi(bar) for bar in data.data_iter)
        elif data.is_dk:
            if symbol_complete not in self.data_dk:
                print_warning(
                    f"Failed to update complete data of {symbol_complete} @ DK - "
                    f"data dict not initialized ({list(self.data_1k.keys())})",
                    force=True
                )
                return

            self.data_dk[symbol_complete].update_all(to_bar_data_dict_tcoreapi(bar) for bar in data.data_iter)
        else:
            raise ValueError(
                f"No data update as the history data is not either 1K or DK - {symbol_complete} @ {data.data_type}"
            )

    def update_latest_market_data_of_symbol(self, data: RealtimeData) -> None:
        if data_1k := self.data_1k.get(data.symbol_complete):
            data_1k.update_latest_market(data)
        if data_dk := self.data_dk.get(data.symbol_complete):
            data_dk.update_latest_market(data)

    def update_market_data_of_symbol(self, data: RealtimeData) -> MarketPxUpdateResult:
        symbol_complete = data.symbol_complete

        reason = None
        if data_1k := self.data_1k.get(symbol_complete):
            reason = data_1k.update_latest(data.last_px) or reason
        if data_dk := self.data_dk.get(symbol_complete):
            reason = data_dk.update_latest(data.last_px) or reason

        now = time.time()
        result = MarketPxUpdateResult(
            allow_send=reason or now - self.last_market_send > MARKET_PX_TIME_GATE_SEC,
            force_send_reason=reason,
            data=self.buffer_mkt_px | {data.security: data},
        )

        if result.allow_send:
            self.last_market_send = now
            self.buffer_mkt_px = {}
        else:
            self.buffer_mkt_px[data.security] = data

        return result

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
            offset = px_data_config.offset

            if not (symbol_complete := self.security_to_symbol_complete.get(security)):
                print_warning(
                    f"Attempt to get the uninitialized Px data of [yellow]{security}[/yellow] @ {period_min}"
                    f"{f' (-{offset})' if offset else ''}"
                )
                continue

            if period_min >= 1440:
                lookup_dk[symbol_complete].append(px_data_config)
            else:
                lookup_1k[symbol_complete].append(px_data_config)

        px_data_list = []

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.data_1k[symbol_complete].to_px_data, px_configs)
                for symbol_complete, px_configs in lookup_1k.items()
            ] + [
                executor.submit(self.data_dk[symbol_complete].to_px_data, px_configs)
                for symbol_complete, px_configs in lookup_dk.items()
            ]

            for future in as_completed(futures):
                px_data_list.extend(future.result())

        return px_data_list

    def make_new_bar(self, data: SystemTimeData):
        if data.epoch_sec == 0:
            for cache_entry in self.data_dk.values():
                print_log(
                    f"[Server] Creating new bar for [yellow]{cache_entry.security}[/yellow] @ [yellow]DK[/yellow] "
                    f"at {data.timestamp}"
                )
                cache_entry.make_new_bar(data.epoch_sec)

        for cache_entry in self.data_1k.values():
            print_log(
                f"[Server] Creating new bar for [yellow]{cache_entry.security}[/yellow] @ [yellow]1K[/yellow] "
                f"at {data.timestamp}"
            )
            cache_entry.make_new_bar(data.epoch_sec)
