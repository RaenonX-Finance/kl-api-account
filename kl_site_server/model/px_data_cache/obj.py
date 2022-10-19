import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, Iterable

from kl_site_common.const import MARKET_PX_TIME_GATE_SEC
from kl_site_common.utils import print_log, print_warning
from kl_site_server.calc import CALC_STRENGTH_BARS_NEEDED, calc_strength
from kl_site_server.db import get_calculated_data_from_db, is_market_closed, store_history_to_db_from_entries
from tcoreapi_mq.message import HistoryData, HistoryInterval, PxHistoryDataEntry, RealtimeData, SystemTimeData
from tcoreapi_mq.model import FUTURES_SECURITY_TO_SYM_OBJ, SymbolBaseType
from .entry import PxDataCacheEntry
from ..bar_data import to_bar_data_dict_tcoreapi
from ..px_data import PxData, PxDataConfig
from ..px_data_update import MarketPxUpdateResult


@dataclass(kw_only=True)
class PxDataCache:
    symbol_obj_in_use: set[SymbolBaseType] = field(init=False, default_factory=set)

    data_1k: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)
    data_dk: dict[str, PxDataCacheEntry] = field(init=False, default_factory=dict)

    last_market_send: float = field(init=False, default=0)

    buffer_mkt_px: dict[str, RealtimeData] = field(init=False, default_factory=dict)  # Security / Data

    def init_entry(
        self, *, symbol_obj: SymbolBaseType, min_tick: float, decimals: int,
        period_mins: list[int], period_days: list[int],
    ) -> None:
        symbol_complete = symbol_obj.symbol_complete

        self.symbol_obj_in_use.add(symbol_obj)

        if period_mins:
            self.data_1k[symbol_complete] = PxDataCacheEntry(
                security=symbol_obj.security,
                symbol_complete=symbol_complete,
                min_tick=min_tick,
                decimals=decimals,
                data={},
                interval="1K",
                interval_sec=60,
            )

        if period_days:
            self.data_dk[symbol_complete] = PxDataCacheEntry(
                security=symbol_obj.security,
                symbol_complete=symbol_complete,
                min_tick=min_tick,
                decimals=decimals,
                data={},
                interval="DK",
                interval_sec=86400,
            )

    def get_last_n_of_close_px(self, security: str, count: int) -> list[float]:
        return self.data_1k[FUTURES_SECURITY_TO_SYM_OBJ[security].symbol_complete].get_last_n_of_close_px(count)

    def get_cache_entry_of_interval(self, interval: HistoryInterval, symbol_complete: str) -> PxDataCacheEntry:
        if interval == "1K":
            return self.data_1k[symbol_complete]

        if interval == "DK":
            return self.data_dk[symbol_complete]

        raise ValueError(f"No associated cache entry available for `{symbol_complete}`")

    @staticmethod
    def _get_px_data_of_interval(
        px_data_config_lookup: defaultdict[str, list[PxDataConfig]],
        data_holder_dict: dict[str, PxDataCacheEntry],
    ) -> list[PxData]:
        px_data_list = []

        calculated_data_lookup = get_calculated_data_from_db(
            px_data_config_lookup.keys(),
            {config.period_min for configs in px_data_config_lookup.values() for config in configs},
            count_override={
                (symbol_complete, px_config.period_min): px_config.limit
                for symbol_complete, px_configs in px_data_config_lookup.items()
                for px_config in px_configs
            },
            offset_override={
                (symbol_complete, px_config.period_min): px_config.offset
                for symbol_complete, px_configs in px_data_config_lookup.items()
                for px_config in px_configs
            },
        )

        for symbol_complete, px_configs in px_data_config_lookup.items():
            px_data_list.extend(data_holder_dict[symbol_complete].to_px_data(px_configs, calculated_data_lookup))

        return px_data_list

    def get_px_data(self, px_data_configs: Iterable[PxDataConfig]) -> list[PxData]:
        lookup_1k, lookup_dk = self._get_px_data_config_to_lookup(px_data_configs)

        return [
            *self._get_px_data_of_interval(lookup_1k, self.data_1k),
            *self._get_px_data_of_interval(lookup_dk, self.data_dk)
        ]

    def update_complete_data_of_symbol(self, data: HistoryData) -> None:
        symbol_complete = data.symbol_complete

        print_log(
            f"Updating [purple]{data.data_len_as_str}[/purple] Px data bars "
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
                    f"data dict not initialized ({list(self.data_dk.keys())})",
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
                security: calc_strength(self.get_last_n_of_close_px(security, CALC_STRENGTH_BARS_NEEDED))
                for security in market_px_data.keys()
            }
        )

        if result.allow_send:
            self.last_market_send = now
            self.buffer_mkt_px = {}
        else:
            self.buffer_mkt_px[data.security] = data

        return result

    def is_all_ready_of_intervals(self, intervals: list[HistoryInterval], symbol_complete: str) -> bool:
        return all(
            self.get_cache_entry_of_interval(interval, symbol_complete).is_ready
            for interval in intervals
        )

    def is_all_px_data_ready(self) -> bool:
        for cache_entry in [*self.data_1k.values(), *self.data_dk.values()]:
            if not cache_entry.is_ready:
                return False

        return True

    @staticmethod
    def _get_px_data_config_to_lookup(
        px_data_configs: Iterable[PxDataConfig]
    ) -> tuple[DefaultDict[str, list[PxDataConfig]], DefaultDict[str, list[PxDataConfig]]]:
        lookup_1k: DefaultDict[str, list[PxDataConfig]] = defaultdict(list)
        lookup_dk: DefaultDict[str, list[PxDataConfig]] = defaultdict(list)

        for px_data_config in px_data_configs:
            symbol_complete = FUTURES_SECURITY_TO_SYM_OBJ[px_data_config.security].symbol_complete

            if px_data_config.period_min >= 1440:
                lookup_dk[symbol_complete].append(px_data_config)
            else:
                lookup_1k[symbol_complete].append(px_data_config)

        return lookup_1k, lookup_dk

    @staticmethod
    def _make_new_bar(
        data: SystemTimeData,
        cache_body: dict[str, PxDataCacheEntry],
        interval: HistoryInterval
    ) -> set[str]:
        securities_created = set()

        new_bars: list[PxHistoryDataEntry] = []

        for cache_entry in cache_body.values():
            if is_market_closed(cache_entry.security):  # https://github.com/RaenonX-Finance/kl-site-back/issues/40
                print_log(
                    f"[red]Skipped[/red] creating new bar of [yellow]{cache_entry.security}[/yellow] - "
                    f"outside market hours"
                )
                continue

            last_px = cache_entry.make_new_bar(data.epoch_sec)

            if not last_px:
                print_log(
                    f"[red]Skipped[/red] creating new bar of [yellow]{cache_entry.security}[/yellow] - "
                    f"no last px"
                )
                continue

            print_log(
                f"Creating new bar for [yellow]{cache_entry.security}[/yellow] "
                f"in [yellow]{interval}[/yellow] at {data.timestamp}"
            )

            new_bars.append(PxHistoryDataEntry.make_new_bar(
                cache_entry.symbol_complete,
                cache_entry.interval,
                data.timestamp,
                last_px
            ))

            securities_created.add(cache_entry.security)

        store_history_to_db_from_entries(new_bars)

        return securities_created

    def make_new_bar(self, data: SystemTimeData) -> set[str]:
        securities_created = set()

        if data.epoch_sec == 0:
            securities_created |= self._make_new_bar(data, self.data_dk, "DK")

        securities_created |= self._make_new_bar(data, self.data_1k, "1K")

        return securities_created
