import time
from typing import Iterable

from kl_site_common.utils import execute_async_function, print_warning
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
        params.reset_request_timeout()
        self._px_request_params[params.symbol_obj.symbol_complete] = params

        hist_start, hist_end = params.history_range

        self.get_history(params.symbol_obj, "1K", hist_start, hist_end)
        self.subscribe_realtime(params.symbol_obj)

        self._px_data_cache.init_entry(
            params.symbol_obj,
            self.get_instrument_info_by_symbol(params.symbol_obj).tick,
            params.period_mins
        )

    def get_all_px_data(self) -> Iterable[PxData]:
        if not self._px_data_cache.is_all_px_data_ready():
            print_warning("[TC Client] `get_all_px_data()` called while not all of the Px data are ready")

        return iter(
            px_data
            for px_cache_entry
            in self._px_data_cache.px_cache_entries
            if px_cache_entry.is_ready
            for px_data, _
            in px_cache_entry.to_px_data(self._px_data_cache.period_mins[px_cache_entry.symbol_complete]).values()
        )

    def send_complete_px_data(self, symbol_complete: str, proc_sec_offset: float) -> bool:
        if not self._px_data_cache.is_send_complete_data_ok(symbol_complete):
            return False

        px_data_list, proc_sec_list = self._px_data_cache.complete_px_data_to_send(symbol_complete)

        proc_sec_list = [proc_sec + proc_sec_offset for proc_sec in proc_sec_list]

        execute_async_function(
            on_px_data_updated,
            OnPxDataUpdatedEvent(px_data_list=px_data_list, proc_sec_list=proc_sec_list),
        )

        self._px_data_cache.mark_complete_data_sent()
        return True

    def send_market_px_data(self, symbol_complete: str, data: RealtimeData) -> None:
        if not self._px_data_cache.is_send_market_data_ok(symbol_complete):
            self._px_data_cache.rec_buffer_market_data(data)
            return

        execute_async_function(
            on_px_data_updated_market,
            OnMarketDataReceivedEvent(data=self._px_data_cache.buffer_market_data | {data.security: data}),
        )

        self._px_data_cache.mark_market_data_sent()

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
