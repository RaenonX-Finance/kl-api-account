import asyncio
import threading
import time
from datetime import datetime, timedelta
from itertools import product
from typing import Callable, Iterable, Iterator, NamedTuple

from pandas import DataFrame

from kl_site_common.const import DATA_PX_REFETCH_BACKWARD_HOUR, DATA_PX_REFETCH_INTERVAL_SEC
from kl_site_common.utils import DataCache, execute_async_function, print_log, print_warning
from kl_site_server.app import (
    on_error, on_px_data_new_bar_created, on_px_data_updated_market, on_system_time_min_change,
)
from kl_site_server.calc import calculate_indicators_full, calculate_indicators_last, calculate_indicators_partial
from kl_site_server.db import (
    StoreCalculatedDataArgs, get_calculated_data_from_db, get_history_data_from_db_full,
    get_history_data_from_db_limit_count, get_history_data_from_db_timeframe, is_market_closed,
    store_calculated_to_db, store_history_to_db,
)
from kl_site_server.model import (
    OnErrorEvent, OnMarketDataReceivedEvent,
    PxData, PxDataCache, PxDataConfig, TouchancePxRequestParams,
)
from kl_site_server.utils import MAX_PERIOD, MAX_PERIOD_NO_EMA
from tcoreapi_mq.client import TouchanceApiClient
from tcoreapi_mq.message import HistoryData, HistoryInterval, PxHistoryDataEntry, RealtimeData, SystemTimeData
from tcoreapi_mq.model import SymbolBaseType


class PeriodIntervalInfo(NamedTuple):
    period_min: int
    interval: HistoryInterval
    max_period_num: int


class TouchanceDataClient(TouchanceApiClient):
    def __init__(self):
        super().__init__()

        self._px_data_cache: PxDataCache = PxDataCache()
        self._px_request_params: dict[str, TouchancePxRequestParams] = {}
        self._update_calculated_data_lock: threading.Lock = threading.Lock()

        threading.Thread(target=self._history_data_refetcher).start()

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
            self._calc_data_update_full(params.symbol_obj)

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
                continue

            for params in self._px_request_params.values():
                if is_market_closed(params.symbol_obj.security):
                    continue

                start = datetime.utcnow() - timedelta(hours=DATA_PX_REFETCH_BACKWARD_HOUR)
                end = datetime.utcnow() + timedelta(minutes=2)

                if params.period_mins:
                    self.get_history_including_db(params.symbol_obj, "1K", start, end)

                if params.period_days:
                    self.get_history_including_db(params.symbol_obj, "DK", start, end)

    def _get_params_interval_info(self) -> set[PeriodIntervalInfo]:
        period_min_set = set()

        max_period_num: dict[HistoryInterval, int] = {
            "1K": max(
                period_min for params in self._px_request_params.values()
                for period_min in params.period_mins
            ),
            "DK": max(
                period_day for params in self._px_request_params.values()
                for period_day in params.period_days
            ),
        }

        for params in self._px_request_params.values():
            period_min_set.update(
                PeriodIntervalInfo(period_min, "1K", max_period_num["1K"])
                for period_min in params.period_mins
            )
            period_min_set.update(
                PeriodIntervalInfo(period_day * 1440, "DK", max_period_num["DK"])
                for period_day in params.period_days
            )

        return period_min_set

    def _calc_data_update_new_bar(
        self,
        fn_get_history_data: Callable[[PeriodIntervalInfo], list[PxHistoryDataEntry]],
        fn_get_cached_calculated_data: Callable[[SymbolBaseType, PeriodIntervalInfo], list[dict] | None],
        symbols: Iterable[SymbolBaseType],
    ) -> None:
        interval_info_list = self._get_params_interval_info()
        history_data_cache = DataCache(fn_get_history_data)
        product_gen: Iterator[tuple[SymbolBaseType, PeriodIntervalInfo]] = product(
            symbols,
            interval_info_list
        )
        store_calculated_args: list[StoreCalculatedDataArgs] = []

        for symbol_obj, interval_info in product_gen:
            cached_calculated_data = fn_get_cached_calculated_data(symbol_obj, interval_info)

            cached_calculated_df = DataFrame(cached_calculated_data) if cached_calculated_data else None
            data_recs = history_data_cache.get_value((symbol_obj, interval_info))

            if cached_calculated_df is not None and data_recs:
                print_log(
                    "[TC Client] Calculating indicators of "
                    f"[yellow]{symbol_obj.security}@{interval_info.period_min}[/yellow] on partial data"
                )
                calculated_df = calculate_indicators_partial(
                    interval_info.period_min,
                    data_recs,
                    cached_calculated_df
                )
                store_calculated_args.append(StoreCalculatedDataArgs(
                    symbol_obj, interval_info.period_min, calculated_df, True
                ))
            else:
                print_warning(
                    "[TC Client] No history or cached calculated data available "
                    f"for [bold]{symbol_obj.symbol_complete}[/bold]@{interval_info.period_min}"
                )

        store_calculated_to_db(store_calculated_args)

    def _calc_data_make_new_bar(self) -> None:
        def get_history_data(key: tuple[SymbolBaseType, PeriodIntervalInfo]) -> list[PxHistoryDataEntry]:
            symbol_obj_, interval_info = key
            data = get_history_data_from_db_limit_count(
                symbol_obj_.symbol_complete,
                interval_info.interval,
                interval_info.max_period_num * MAX_PERIOD_NO_EMA
            ).data

            return data

        def get_cached_calculated_data(
            symbol_obj: SymbolBaseType,
            interval_info: PeriodIntervalInfo
        ) -> list[dict] | None:
            return list(get_calculated_data_from_db(
                symbol_obj, interval_info.period_min,
                count=MAX_PERIOD_NO_EMA
            ))

        self._calc_data_update_new_bar(
            get_history_data,
            get_cached_calculated_data,
            self._px_data_cache.symbol_obj_in_use,
        )

    def _calc_data_update_common(
        self,
        fn_get_history_data: Callable[[PeriodIntervalInfo], list[PxHistoryDataEntry]],
        fn_get_cached_calculated_data: Callable[[SymbolBaseType, PeriodIntervalInfo], tuple[list[dict] | None, bool]],
        symbols: Iterable[SymbolBaseType], *,
        use_thread: bool,
        skip_if_locked: bool = True,
    ) -> None:
        def update_calculated_data_exec() -> None:
            if skip_if_locked and self._update_calculated_data_lock.locked():
                return

            with self._update_calculated_data_lock:
                interval_info_list = self._get_params_interval_info()
                history_data_cache = DataCache(fn_get_history_data)
                product_gen: Iterator[tuple[SymbolBaseType, PeriodIntervalInfo]] = product(
                    symbols,
                    interval_info_list
                )
                store_calculated_args: list[StoreCalculatedDataArgs] = []

                for symbol_obj, interval_info in product_gen:
                    cached_calculated_data, full_update = fn_get_cached_calculated_data(symbol_obj, interval_info)
                    cached_calculated_df = DataFrame(cached_calculated_data) if cached_calculated_data else None

                    calculated_df = None
                    # Calculated data might not have newer bars
                    if cached_calculated_df is not None:
                        print_log(
                            "[TC Client] Calculating indicators of "
                            f"[yellow]{symbol_obj.security}@{interval_info.period_min}[/yellow] on last bar"
                        )
                        calculated_df = calculate_indicators_last(interval_info.period_min, cached_calculated_df)
                    elif data_recs := history_data_cache.get_value((symbol_obj, interval_info)):
                        print_log(
                            "[TC Client] Calculating indicators of "
                            f"[yellow]{symbol_obj.security}@{interval_info.period_min}[/yellow] on all data"
                        )
                        calculated_df = calculate_indicators_full(interval_info.period_min, data_recs)

                    if calculated_df is not None:
                        store_calculated_args.append(StoreCalculatedDataArgs(
                            symbol_obj, interval_info.period_min, calculated_df, full_update
                        ))
                    else:
                        print_warning(
                            "[TC Client] No history or cached calculated data available "
                            f"for [bold]{symbol_obj.symbol_complete}[/bold]@{interval_info.period_min}"
                        )

                store_calculated_to_db(store_calculated_args)

        if use_thread:
            threading.Thread(target=update_calculated_data_exec).start()
        else:
            update_calculated_data_exec()

    def _calc_data_update_last(self) -> None:
        def get_history_data(key: tuple[SymbolBaseType, PeriodIntervalInfo]) -> list[PxHistoryDataEntry]:
            symbol_obj_, interval_info = key
            return get_history_data_from_db_full(symbol_obj_.symbol_complete, interval_info.interval).data

        def get_cached_calculated_data(
            symbol_obj: SymbolBaseType,
            interval_info: PeriodIntervalInfo
        ) -> tuple[list[dict] | None, bool]:
            cached_calculated_data = list(get_calculated_data_from_db(
                symbol_obj, interval_info.period_min,
                count=MAX_PERIOD
            ))
            full_update = not cached_calculated_data

            return cached_calculated_data, full_update

        self._calc_data_update_common(
            get_history_data,
            get_cached_calculated_data,
            self._px_data_cache.symbol_obj_in_use,
            use_thread=True
        )

    def _calc_data_update_full(self, symbol_obj: SymbolBaseType) -> None:
        def get_history_data(key: tuple[SymbolBaseType, PeriodIntervalInfo]) -> list[PxHistoryDataEntry]:
            symbol_obj_, interval_info = key
            return get_history_data_from_db_full(symbol_obj_.symbol_complete, interval_info.interval).data

        def get_cached_calculated_data(_: SymbolBaseType, __: PeriodIntervalInfo) -> tuple[None, bool]:
            return None, True

        print_log(f"[TC Client] [blue]Started data re-calculation of [yellow]{symbol_obj.security}[/yellow][/blue]")
        self._calc_data_update_common(
            get_history_data,
            get_cached_calculated_data,
            {symbol_obj},
            use_thread=False,
            skip_if_locked=False,
        )
        print_log(f"[TC Client] [blue]Completed data re-calculation of [yellow]{symbol_obj.security}[/yellow][/blue]")

    def on_received_history_data(self, data: HistoryData) -> None:
        print_log(
            f"[TC Client] Received history data of [yellow]{data.symbol_complete}[/yellow] "
            f"at [yellow]{data.data_type}[/yellow]"
        )
        store_history_to_db(data)
        self._px_data_cache.update_complete_data_of_symbol(data)

        self._calc_data_update_last()

    def on_received_realtime_data(self, data: RealtimeData) -> None:
        if is_market_closed(data.security):  # https://github.com/RaenonX-Finance/kl-site-back/issues/40
            print_log(
                f"[TC Client] [red]Ignoring[/red] market Px data of [yellow]{data.security}[/yellow] - "
                f"outside market hours"
            )
            return

        self._px_data_cache.update_latest_market_data_of_symbol(data)

        if not self._px_data_cache.is_px_data_ready(data.symbol_complete):
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

        self._calc_data_update_last()
        execute_async_function(on_px_data_updated_market, OnMarketDataReceivedEvent(result=update_result))

    def on_system_time_min_change(self, data: SystemTimeData) -> None:
        securities_created = self._px_data_cache.make_new_bar(data)

        self._calc_data_make_new_bar()

        execute_async_function(
            asyncio.gather,
            on_system_time_min_change(data),
            on_px_data_new_bar_created(self, securities_created)
        )

    def on_error(self, message: str) -> None:
        execute_async_function(on_error, OnErrorEvent(message=message))
