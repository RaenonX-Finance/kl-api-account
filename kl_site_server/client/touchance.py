import time
from typing import Iterable

from kl_site_common.utils import execute_async_function, print_log, print_warning
from kl_site_server.app import on_error, on_px_data_updated, on_px_data_updated_market
from kl_site_server.model import (
    OnErrorEvent, OnMarketDataReceivedEvent, OnPxDataUpdatedEvent, PxData, PxDataCache, TouchancePxRequestParams,
)
from tcoreapi_mq.client import TouchanceApiClient
from tcoreapi_mq.message import HistoryData, RealtimeData


class TouchanceDataClient(TouchanceApiClient):
    def __init__(self):
        super().__init__()

        self._px_data_cache: PxDataCache = PxDataCache()
        self._px_request_params: dict[str, TouchancePxRequestParams] = {}

    def request_px_data(self, params: TouchancePxRequestParams) -> None:
        # Record params
        params.reset_request_timeout()
        self._px_request_params[params.symbol_obj.symbol_complete] = params

        hist_start, hist_end = params.history_range

        self.get_history(params.symbol_obj, "1K", hist_start, hist_end)
        self.subscribe_realtime(params.symbol_obj)

        product_info = self.get_instrument_info_by_symbol(params.symbol_obj)

        for period_min in params.period_mins:
            self._px_data_cache.init_entry(params.symbol_obj, product_info.tick, period_min)

    def get_all_px_data(self) -> Iterable[PxData]:
        if not self._px_data_cache.is_all_px_data_ready():
            print_warning("[TC Client] `get_all_px_data()` called while not all of the Px data are ready")

        return iter(
            px_cache_entry.to_px_data()
            for px_cache_entry in self._px_data_cache.px_cache_entries
            if px_cache_entry.is_ready
        )

    def send_complete_px_data(self, symbol_complete: str, proc_sec_offset: float) -> bool:
        complete_px_sent = 0

        for px_data, proc_sec_single in self._px_data_cache.complete_px_data_to_send(symbol_complete):
            complete_px_sent += 1

            execute_async_function(
                on_px_data_updated,
                OnPxDataUpdatedEvent(px_data=px_data, proc_sec=proc_sec_offset + proc_sec_single),
            )

        if complete_px_sent:
            self._px_data_cache.mark_complete_data_sent(symbol_complete)
            print_log(f"[TC Client] {complete_px_sent} complete Px data of [yellow]{symbol_complete}[/yellow] sent")

        return complete_px_sent > 0

    def send_market_px_data(self, symbol_complete: str, data: RealtimeData) -> None:
        if not self._px_data_cache.is_send_market_data_ok(symbol_complete):
            return

        self._px_data_cache.mark_market_data_sent(symbol_complete)

        execute_async_function(
            on_px_data_updated_market,
            OnMarketDataReceivedEvent(data=data),
        )

    def on_received_history_data(self, data: HistoryData) -> None:
        _start = time.time()

        self._px_data_cache.update_complete_data_of_symbol(data)

        proc_sec_update = time.time() - _start

        self.send_complete_px_data(data.handshake.symbol_complete, proc_sec_update)

    def on_received_realtime_data(self, data: RealtimeData) -> None:
        self._px_data_cache.update_latest_market_data_of_symbol(data)

        if not self._px_data_cache.is_px_data_ready(data.symbol_complete):
            params = self._px_request_params[data.symbol_complete]

            if params.should_re_request:
                print_warning(f"[TC Client] Re-requesting Px data of {data.symbol_complete}")
                self.request_px_data(params)

            return

        self._px_data_cache.update_market_data_of_symbol(data)

        if not self.send_complete_px_data(data.symbol_complete, 0):
            # Only send market px data if none of complete px data is sent
            self.send_market_px_data(data.symbol_complete, data)

    def on_error(self, message: str) -> None:
        e = OnErrorEvent(message=message)
        execute_async_function(on_error, (e,))
