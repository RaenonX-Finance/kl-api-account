import time
from datetime import datetime
from typing import Iterable

from kl_site_common.utils import execute_async_function, print_warning
from kl_site_server.app import on_error, on_px_data_updated, on_px_data_updated_market
from kl_site_server.model import (
    OnErrorEvent, OnMarketDataReceivedEvent, OnPxDataUpdatedEvent, PxData, PxDataCache,
)
from tcoreapi_mq.client import TocuhanceApiClient
from tcoreapi_mq.message import HistoryData, RealtimeData
from tcoreapi_mq.model import SymbolBaseType


class TouchanceDataClient(TocuhanceApiClient):
    def __init__(self):
        super().__init__()

        self._px_data_cache: PxDataCache = PxDataCache()

    def request_px_data(
            self, symbol_obj: SymbolBaseType, period_mins: list[int], history_range: tuple[datetime, datetime]
    ) -> None:
        hist_start, hist_end = history_range

        self.get_history(symbol_obj, "1K", hist_start, hist_end)
        self.subscribe_realtime(symbol_obj)

        product_info = self.get_instrument_info_by_symbol(symbol_obj)

        for period_min in period_mins:
            self._px_data_cache.init_entry(symbol_obj, product_info.tick, period_min // 60)

    def get_all_px_data(self) -> Iterable[PxData]:
        if not self._px_data_cache.is_all_px_data_ready():
            print_warning(f"`get_all_px_data()` called while not all of the Px data are ready")

        return iter(
            px_cache_entry.to_px_data()
            for px_cache_entry in self._px_data_cache.px_cache_entries
            if px_cache_entry.is_ready
        )

    def send_complete_px_data(self, symbol_complete: str, proc_sec_offset: float) -> None:
        for px_data, proc_sec_single in self._px_data_cache.complete_px_data_to_send(symbol_complete):
            execute_async_function(
                on_px_data_updated,
                OnPxDataUpdatedEvent(px_data=px_data, proc_sec=proc_sec_offset + proc_sec_single),
            )

    def send_market_px_data(self, symbol_complete: str, data: RealtimeData) -> None:
        if not self._px_data_cache.is_send_market_data_ok(symbol_complete):
            return

        execute_async_function(
            on_px_data_updated_market,
            OnMarketDataReceivedEvent(symbol=data.security, px=data.last_px),
        )

    def on_received_history_data(self, data: HistoryData) -> None:
        _start = time.time()

        self._px_data_cache.update_complete_data_of_symbol(data)

        proc_sec_update = time.time() - _start

        self.send_complete_px_data(data.handshake.symbol_complete, proc_sec_update)

    def on_received_realtime_data(self, data: RealtimeData) -> None:
        if not self._px_data_cache.is_px_data_ready(data.symbol_complete):
            print_warning(f"[TC Client] Px data of [purple]{data.symbol_complete}[/purple] not ready")
            return

        self._px_data_cache.update_market_data_of_symbol(data)

        self.send_complete_px_data(data.symbol_complete, 0)
        self.send_market_px_data(data.symbol_complete, data)

    def on_error(self, message: str) -> None:
        e = OnErrorEvent(message=message)
        execute_async_function(on_error, (e,))
