import asyncio
import time
from datetime import datetime, timedelta
from threading import Thread

from kl_site_common.const import DATA_PX_REFETCH_BACKWARD_HOUR, DATA_PX_REFETCH_INTERVAL_SEC
from kl_site_common.utils import execute_async_function, print_log, print_warning
from kl_site_server.app import (
    on_error, on_px_data_new_bar_created, on_px_data_updated_market, on_system_time_min_change,
)
from kl_site_server.db import get_history_data_from_db_timeframe, is_market_closed, store_history_to_db
from kl_site_server.model import (
    OnErrorEvent, OnMarketDataReceivedEvent,
    PxData, PxDataCache, PxDataConfig, TouchancePxRequestParams,
)
from tcoreapi_mq.client import TouchanceApiClient
from tcoreapi_mq.message import HistoryData, HistoryInterval, RealtimeData, SystemTimeData
from tcoreapi_mq.model import SymbolBaseType
from .calc_data import CalculatedDataManager


class TouchanceDataClient(TouchanceApiClient):
    def __init__(self):
        super().__init__()

        self._px_data_cache: PxDataCache = PxDataCache()
        self._px_request_params: dict[str, TouchancePxRequestParams] = {}
        self._calc_data_manager: CalculatedDataManager = CalculatedDataManager(
            self._px_data_cache, self._px_request_params
        )

        Thread(target=self._history_data_refetcher).start()

    def request_px_data(self, params: TouchancePxRequestParams, *, re_calc_data: bool) -> None:
        if not params.period_mins and not params.period_days:
            raise ValueError("Both `period_mins` or `period_days` in `params` cannot be 0 length at the same time")

        params.reset_request_timeout()
        self._px_request_params[params.symbol_obj.symbol_complete] = params

        instrument_info = self.get_symbol_info(params.symbol_obj)

        self._px_data_cache.init_entry(
            symbol_obj=params.symbol_obj,
            min_tick=instrument_info.tick,
            decimals=instrument_info.decimals,
            period_mins=params.period_mins,
            period_days=params.period_days
        )

        if params.period_mins:
            self.get_history_including_db(params.symbol_obj, "1K", *params.history_range_1k)

        if params.period_days:
            self.get_history_including_db(params.symbol_obj, "DK", *params.history_range_dk)

        # Needs to be placed before `subscribe_realtime`
        if re_calc_data:
            # Ensure all history data requests are finished
            # Not using context manager because sometimes it unlocked locked lock
            self.history_data_lock_dict[params.symbol_obj.symbol_complete].acquire()
            self._calc_data_manager.update_calc_data_full(params.symbol_obj)
            if self.history_data_lock_dict[params.symbol_obj.symbol_complete].locked():
                self.history_data_lock_dict[params.symbol_obj.symbol_complete].release()

        self.subscribe_realtime(params.symbol_obj)

    def get_history_including_db(
        self,
        symbol: SymbolBaseType,
        interval: HistoryInterval,
        start: datetime,
        end: datetime
    ):
        symbol_complete = symbol.symbol_complete

        result = get_history_data_from_db_timeframe(symbol_complete, interval, start, end)

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
                print_warning("[TC Client] Skipped re-fetching history px data - not all px data are ready")
                continue

            # Create list to avoid size change during iteration error
            for params in list(self._px_request_params.values()):
                if is_market_closed(params.symbol_obj.security):
                    print_log(
                        f"[TC Client] Skipped re-fetching history of "
                        f"[yellow]{params.symbol_obj.security}[/yellow] - [red]market closed[/red]"
                    )
                    continue

                start = datetime.utcnow() - timedelta(hours=DATA_PX_REFETCH_BACKWARD_HOUR)
                end = datetime.utcnow() + timedelta(minutes=2)

                print_log(f"[TC Client] Re-fetching history of [yellow]{params.symbol_obj.security}[/yellow]")

                if params.period_mins:
                    self.get_history(params.symbol_obj, "1K", start, end, ignore_lock=True)

                if params.period_days:
                    self.get_history(params.symbol_obj, "DK", start, end, ignore_lock=True)

    def on_received_history_data(self, data: HistoryData) -> None:
        print_log(
            f"[TC Client] Received history data of [yellow]{data.symbol_complete}[/yellow] "
            f"at [yellow]{data.data_type}[/yellow]"
        )

        store_history_to_db(data)
        self._px_data_cache.update_complete_data_of_symbol(data)

        self._calc_data_manager.update_calc_data_last()

    def on_received_realtime_data(self, data: RealtimeData) -> None:
        if is_market_closed(data.security):  # https://github.com/RaenonX-Finance/kl-site-back/issues/40
            print_log(
                f"[TC Client] [red]Ignoring[/red] market Px data of [yellow]{data.security}[/yellow] - "
                f"outside market hours"
            )
            return

        self._px_data_cache.update_latest_market_data_of_symbol(data)

        if not self._px_data_cache.is_all_ready_of_intervals(["1K", "DK"], data.symbol_complete):
            params = self._px_request_params[data.symbol_complete]

            if params.should_re_request:
                print_warning(f"[TC Client] Re-requesting Px data of {data.security}")
                self.request_px_data(params, re_calc_data=False)

            return

        update_result = self._px_data_cache.update_market_data_of_symbol(data)

        if not update_result.allow_send:
            return

        if update_result.is_force_send:
            print_log(
                f"[TC Client] [yellow]Force-sending[/yellow] market Px - "
                f"Reason: [blue]{update_result.force_send_reason}[/blue]"
            )

        self._calc_data_manager.update_calc_data_last()
        execute_async_function(on_px_data_updated_market, OnMarketDataReceivedEvent(result=update_result))

    def on_system_time_min_change(self, data: SystemTimeData) -> None:
        securities_created = self._px_data_cache.make_new_bar(data)

        self._calc_data_manager.update_calc_data_new_bar()

        execute_async_function(
            asyncio.gather,
            on_system_time_min_change(data),
            on_px_data_new_bar_created(self, securities_created)
        )

    def on_error(self, message: str) -> None:
        execute_async_function(on_error, OnErrorEvent(message=message))
