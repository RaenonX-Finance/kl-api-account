import threading
import time
from datetime import datetime, timedelta

from kl_site_common.const import DATA_PX_REFETCH_BACKWARD_HOUR, DATA_PX_REFETCH_INTERVAL_SEC
from kl_site_common.utils import execute_async_function, print_warning
from kl_site_server.app import on_error, on_px_data_updated_market
from kl_site_server.db import get_history_data_from_db, store_history_to_db
from kl_site_server.model import (
    OnErrorEvent, OnMarketDataReceivedEvent,
    PxData, PxDataCache, PxDataConfig, TouchancePxRequestParams,
)
from tcoreapi_mq.client import TouchanceApiClient
from tcoreapi_mq.message import HistoryData, HistoryInterval, RealtimeData, SystemTimeData
from tcoreapi_mq.model import SymbolBaseType


class TouchanceDataClient(TouchanceApiClient):
    def __init__(self):
        super().__init__()

        self._px_data_cache: PxDataCache = PxDataCache()
        self._px_request_params: dict[str, TouchancePxRequestParams] = {}

        threading.Thread(target=self._history_data_refetcher).start()

    def request_px_data(self, params: TouchancePxRequestParams) -> None:
        if not params.period_mins and not params.period_days:
            raise ValueError("Both `period_mins` or `period_days` in `params` cannot be 0 length at the same time")

        params.reset_request_timeout()
        self._px_request_params[params.symbol_obj.symbol_complete] = params

        hist_start, hist_end = params.history_range

        self.register_symbol_info(params.symbol_obj)
        instrument_info = self.get_instrument_info_by_symbol(params.symbol_obj)

        self._px_data_cache.init_entry(
            symbol_obj=params.symbol_obj,
            min_tick=instrument_info.tick,
            decimals=instrument_info.decimals,
            period_mins=params.period_mins,
            period_days=params.period_days
        )

        if params.period_mins:
            self.get_history_including_db(params.symbol_obj, "1K", hist_start, hist_end)

        if params.period_days:
            self.get_history_including_db(params.symbol_obj, "DK", hist_start, hist_end)

        self.subscribe_realtime(params.symbol_obj)

    def get_history_including_db(
        self,
        symbol: SymbolBaseType,
        interval: HistoryInterval,
        start: datetime,
        end: datetime
    ):
        symbol_complete = symbol.symbol_complete

        result = get_history_data_from_db(symbol_complete, interval, start, end)

        self._px_data_cache.update_complete_data_of_symbol(HistoryData.from_db_fetch(
            symbol_complete, interval, result
        ))

        if not result.earliest and not result.latest:
            self.get_history(symbol, interval, start, end)
        else:
            self.get_history(symbol, interval, start, result.earliest)
            self.get_history(symbol, interval, result.latest, end)

    def get_px_data(self, px_data_configs: set[PxDataConfig]) -> list[PxData]:
        return self._px_data_cache.get_px_data(px_data_configs)

    def _history_data_refetcher(self):
        while True:
            time.sleep(DATA_PX_REFETCH_INTERVAL_SEC)
            if not self._px_data_cache.is_all_px_data_ready():
                continue

            for params in self._px_request_params.values():
                start = datetime.utcnow() - timedelta(hours=DATA_PX_REFETCH_BACKWARD_HOUR)
                end = datetime.utcnow() + timedelta(minutes=2)

                if params.period_mins:
                    self.get_history_including_db(params.symbol_obj, "1K", start, end)

                if params.period_days:
                    self.get_history_including_db(params.symbol_obj, "DK", start, end)

    def on_received_history_data(self, data: HistoryData) -> None:
        store_history_to_db(data)
        self._px_data_cache.update_complete_data_of_symbol(data)

    def on_received_realtime_data(self, data: RealtimeData) -> None:
        self._px_data_cache.update_latest_market_data_of_symbol(data)

        if not self._px_data_cache.is_px_data_ready(data.symbol_complete):
            params = self._px_request_params[data.symbol_complete]

            if params.should_re_request:
                print_warning(f"[TC Client] Re-requesting Px data of {data.security}")
                self.request_px_data(params)

            return

        self._px_data_cache.update_market_data_of_symbol(data)

        # `send_market_px_data()` must place before `send_complete_px_data()`
        # because the latter takes time to calc
        execute_async_function(on_px_data_updated_market, OnMarketDataReceivedEvent(data=data))

    def on_system_time_min_change(self, data: SystemTimeData) -> None:
        pass

    def on_error(self, message: str) -> None:
        execute_async_function(on_error, OnErrorEvent(message=message))
