import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import DefaultDict, Iterable

from kl_site_common.const import MARKET_PX_TIME_GATE_SEC
from kl_site_common.utils import print_log, print_warning
from kl_site_server.calc import calc_strength
from kl_site_server.db import is_market_closed
from kl_site_server.utils import MAX_PERIOD
from tcoreapi_mq.message import HistoryData, HistoryInterval, RealtimeData, SystemTimeData
from tcoreapi_mq.model import COMPLETE_SYMBOL_TO_SYM_OBJ, FUTURES_SECURITY_TO_SYM_OBJ, SymbolBaseType
from .entry import PxDataCacheEntry
from .type import HistoryDataFetcherCallable
from ..bar_data import to_bar_data_dict_tcoreapi
from ..px_data import PxData, PxDataConfig
from ..px_data_update import MarketPxUpdateResult


@dataclass(kw_only=True)
class PxDataCache:
    data_1k: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)
    data_dk: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)

    last_market_send: float = field(init=False, default=0)

    period_mins: DefaultDict[str, list[int]] = field(init=False, default_factory=lambda: defaultdict(list))
    period_days: DefaultDict[str, list[int]] = field(init=False, default_factory=lambda: defaultdict(list))

    security_to_symbol_complete: dict[str, str] = field(init=False, default_factory=dict)

    buffer_mkt_px: dict[str, RealtimeData] = field(init=False, default_factory=dict)  # Security / Data

    complete_data_request_lock: set[str] = field(init=False, default_factory=set)

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

    def update_complete_data_of_symbol(self, data: HistoryData, *, is_touchance_response: bool) -> None:
        symbol_complete = data.symbol_complete

        if is_touchance_response:
            self.complete_data_request_lock.discard(symbol_complete)

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
        market_px_data = self.buffer_mkt_px | {data.security: data}
        result = MarketPxUpdateResult(
            allow_send=reason or now - self.last_market_send > MARKET_PX_TIME_GATE_SEC,
            force_send_reason=reason,
            data=market_px_data,
            strength={
                security: calc_strength(
                    # `calc_strength` needs 50 bars only for current setup
                    self.data_1k[FUTURES_SECURITY_TO_SYM_OBJ[security].symbol_complete].get_last_n_of_close_px(70)
                )
                for security in market_px_data.keys()
            }
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

    def _get_px_data_config_to_lookup(
        self, px_data_configs: Iterable[PxDataConfig]
    ) -> tuple[DefaultDict[str, list[PxDataConfig]], DefaultDict[str, list[PxDataConfig]]]:
        lookup_1k: DefaultDict[str, list[PxDataConfig]] = defaultdict(list)
        lookup_dk: DefaultDict[str, list[PxDataConfig]] = defaultdict(list)

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

        return lookup_1k, lookup_dk

    def _send_data_request_if_needed_and_wait(
        self,
        max_group_count: int,
        px_data_cache_body: dict[str, PxDataCacheEntry],
        earliest_ts_dict: dict[str, datetime],
        request_history_data: HistoryDataFetcherCallable,
        interval: HistoryInterval,
        symbols_to_include: Iterable[str],
    ):
        # x2 because simply using `timedelta` includes weekends, but weekends don't have bars
        push_back_mins = max_group_count * MAX_PERIOD * 2 * (1440 if interval == "DK" else 1)

        for symbol, px_cache_entry in px_data_cache_body.items():
            if symbol not in symbols_to_include:
                continue

            end_ts = earliest_ts_dict[symbol]
            start_ts = end_ts - timedelta(minutes=push_back_mins)

            self.complete_data_request_lock.add(symbol)

            request_history_data(COMPLETE_SYMBOL_TO_SYM_OBJ[symbol], interval, start_ts, end_ts)
            print_log(
                f"[Server] Requested [blue]additional[/blue] history [yellow]{interval}[/yellow] data "
                f"of [yellow]{symbol}[/yellow] from {start_ts} to {end_ts}"
            )

        request_sent_epoch_sec = time.time()

        while (
            any(symbol in self.complete_data_request_lock for symbol in px_data_cache_body.keys()) and
            time.time() - request_sent_epoch_sec < 10
        ):
            print_warning(f"[Server] Waiting additional history data of {self.complete_data_request_lock}")
            time.sleep(0.5)

        self.complete_data_request_lock = set()

    def get_px_data(
        self,
        px_data_configs: Iterable[PxDataConfig],
        _: HistoryDataFetcherCallable,
    ) -> list[PxData]:
        lookup_1k, lookup_dk = self._get_px_data_config_to_lookup(px_data_configs)

        # FIXME: Temporarily disables as the feature is incomplete, which could disrupt the data
        # self._send_data_request_if_needed_and_wait(
        #     max(DATA_PERIOD_MINS, key=lambda entry: entry["min"])["min"],
        #     self.data_1k,
        #     {
        #         symbol: min(configs, key=lambda item: item.earliest_ts).earliest_ts
        #         for symbol, configs in lookup_1k.items()
        #     },
        #     request_history_data,
        #     "1K",
        #     lookup_1k.keys(),
        # )
        # self._send_data_request_if_needed_and_wait(
        #     max(DATA_PERIOD_DAYS, key=lambda entry: entry["day"])["day"],
        #     self.data_dk,
        #     {
        #         symbol: min(configs, key=lambda item: item.earliest_ts).earliest_ts
        #         for symbol, configs in lookup_dk.items()
        #     },
        #     request_history_data,
        #     "DK",
        #     lookup_dk.keys(),
        # )

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

    def _make_new_bar(
        self,
        data: SystemTimeData,
        cache_body: dict[str, PxDataCacheEntry],
        interval: HistoryInterval
    ) -> set[str]:
        securities_created = set()

        for cache_entry in cache_body.values():
            if is_market_closed(cache_entry.security):  # https://github.com/RaenonX-Finance/kl-site-back/issues/40
                print_log(
                    f"[Server] [red]Skipped[/red] creating new bar of [yellow]{cache_entry.security}[/yellow] - "
                    f"outside market hours"
                )
                continue

            print_log(
                f"[Server] Creating new bar for [yellow]{cache_entry.security}[/yellow] "
                f"in [yellow]{interval}[/yellow] at {data.timestamp}"
            )
            cache_entry.make_new_bar(data.epoch_sec)
            securities_created.add(cache_entry.security)

        return securities_created

    def make_new_bar(self, data: SystemTimeData) -> set[str]:
        securities_created = set()

        if data.epoch_sec == 0:
            securities_created |= self._make_new_bar(data, self.data_dk, "DK")

        securities_created |= self._make_new_bar(data, self.data_1k, "1K")

        return securities_created
