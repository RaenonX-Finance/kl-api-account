import asyncio
from datetime import datetime

from kl_site_common.const import DATA_PX_REFETCH_STORE_LIMIT
from kl_site_common.utils import execute_async_function, print_log, print_warning
from kl_site_server.app import (
    on_error, on_px_data_new_bar_created, on_px_data_updated_market, on_system_time_min_change,
)
from kl_site_server.db import get_history_data_from_db_timeframe, is_market_closed, store_history_to_db
from kl_site_server.model import (
    OnErrorEvent, OnMarketDataReceivedEvent, PxData, PxDataCache, PxDataConfig,
    TouchancePxRequestParams,
)
from tcoreapi_mq.client import TouchanceApiClient
from tcoreapi_mq.message import HistoryData, HistoryDataHandshake, HistoryInterval, RealtimeData, SystemTimeData
from tcoreapi_mq.model import FUTURES_SECURITY_TO_SYM_OBJ, SymbolBaseType
from .calc_data import CalculatedDataManager
from .subscribe import HistoryDataSubscriber


class TouchanceDataClient(TouchanceApiClient):
    def __init__(self):
        super().__init__()

        self._px_data_cache: PxDataCache = PxDataCache()
        self._px_request_params: dict[str, TouchancePxRequestParams] = {}
        self._requesting_px_data: bool = False
        self._calc_data_manager: CalculatedDataManager = CalculatedDataManager(self._px_data_cache)

    def request_px_data(self, params_list: list[TouchancePxRequestParams], *, re_calc_data: bool) -> None:
        self._requesting_px_data = True

        for params in params_list:
            if not params.period_mins and not params.period_days:
                raise ValueError(
                    "Both `period_mins` or `period_days` in `params` "
                    "cannot be 0 length at the same time"
                )

            params.reset_request_timeout()

            instrument_info = self.get_symbol_info(params.symbol_obj)

            self._px_data_cache.init_entry(
                symbol_obj=params.symbol_obj,
                min_tick=instrument_info.tick,
                decimals=instrument_info.decimals,
                period_mins=params.period_mins,
                period_days=params.period_days
            )

            if params.period_mins:
                self.get_history_including_db(params.symbol_obj, "1K", *params.history_range_1k, subscribe=False)

            if params.period_days:
                self.get_history_including_db(params.symbol_obj, "DK", *params.history_range_dk, subscribe=False)

            # Needs to be placed before `subscribe_realtime`
            if re_calc_data:
                # Ensure all history data requests are finished
                # Not using context manager because sometimes it unlocks locked lock
                self.history_data_lock_dict[params.symbol_obj.symbol_complete].acquire()
                self._calc_data_manager.update_calc_data_full(params.symbol_obj, [params])
                if self.history_data_lock_dict[params.symbol_obj.symbol_complete].locked():
                    self.history_data_lock_dict[params.symbol_obj.symbol_complete].release()

        # Those actions should only happen after complete data calculation
        for params in params_list:
            self.subscribe_realtime(params.symbol_obj)

            # Params should be recorded only after all the calls are done
            self._px_request_params[params.symbol_obj.symbol_complete] = params

        def get_history_data(
            symbol_obj: SymbolBaseType, interval: HistoryInterval, start: datetime, end: datetime
        ):
            self.get_history(symbol_obj, interval, start, end, ignore_lock=True)

        HistoryDataSubscriber(self._px_data_cache, self._px_request_params, get_history_data).start()

        self._requesting_px_data = False

    def get_history_including_db(
        self,
        symbol: SymbolBaseType,
        interval: HistoryInterval,
        start: datetime,
        end: datetime, *,
        subscribe: bool,
    ):
        symbol_complete = symbol.symbol_complete

        result = get_history_data_from_db_timeframe(symbol_complete, interval, start, end)

        history_data = HistoryData.from_db_fetch(symbol_complete, interval, result)
        if history_data.data_list:
            self._px_data_cache.update_complete_data_of_symbol(history_data)
        else:
            identifier = HistoryDataHandshake.make_request_identifier_for_log(
                symbol.symbol_complete, interval, start, end
            )

            print_log("History data unavailable in DB", identifier=identifier)

        if not result.earliest and not result.latest:
            self.get_history(symbol, interval, start, end, subscribe=subscribe)
        else:
            self.get_history(symbol, interval, start, result.earliest, subscribe=subscribe)
            self.get_history(symbol, interval, result.latest, end, subscribe=subscribe)

    def get_px_data(self, px_data_configs: set[PxDataConfig]) -> list[PxData]:
        return self._px_data_cache.get_px_data({
            px_data_config for px_data_config in px_data_configs
            if px_data_config.security in FUTURES_SECURITY_TO_SYM_OBJ
        })

    def on_received_history_data(self, data: HistoryData, handshake: HistoryDataHandshake) -> None:
        last_bar = data.data_list[-1]

        print_log(
            f"Received history data (#{data.data_len} / {last_bar.close} @ {last_bar.timestamp})",
            identifier=handshake.request_identifier,
        )

        store_history_to_db(data, None if self._requesting_px_data else DATA_PX_REFETCH_STORE_LIMIT)
        self._px_data_cache.update_complete_data_of_symbol(data)

        self._calc_data_manager.update_calc_data_last(self._px_request_params.values(), {data.symbol_complete})

        if not self._requesting_px_data:
            self.test_minute_change(data.data_list[-1].timestamp)

    def on_received_realtime_data(self, data: RealtimeData) -> None:
        if is_market_closed(data.security):  # https://github.com/RaenonX-Finance/kl-site-back/issues/40
            print_log(f"[red]Ignored[/] market Px data of [yellow]{data.security}[/] - outside market hours")
            return

        self._px_data_cache.update_latest_market_data_of_symbol(data)

        if not self._px_data_cache.is_all_ready_of_intervals(["1K", "DK"], data.symbol_complete):
            params = self._px_request_params[data.symbol_complete]

            if params.should_re_request:
                print_warning(f"Re-requesting Px data of {data.security}")
                self.request_px_data([params], re_calc_data=False)

            return

        update_result = self._px_data_cache.update_market_data_of_symbol(data)

        if not update_result.allow_send:
            return

        if update_result.is_force_send:
            print_log(f"[yellow]Force-sending[/] market Px - Reason: [blue]{update_result.force_send_reason}[/]")

        execute_async_function(on_px_data_updated_market, OnMarketDataReceivedEvent(result=update_result))

    def on_system_time_min_change(self, data: SystemTimeData) -> None:
        print_log("Server minute change - making new bar on cache")
        securities_created = self._px_data_cache.make_new_bar(data)

        print_log("Server minute change - making new bar for calculated data")
        self._calc_data_manager.update_calc_data_new_bar(self._px_request_params.values())

        execute_async_function(
            asyncio.gather,
            on_system_time_min_change(data),
            on_px_data_new_bar_created(self, securities_created)
        )

    def on_error(self, message: str) -> None:
        execute_async_function(on_error, OnErrorEvent(message=message))
