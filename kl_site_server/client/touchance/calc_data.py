from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from threading import Lock
from typing import Callable, Iterable, NamedTuple, TypeAlias

from pandas import DataFrame

from kl_site_common.utils import DataCache, print_log, print_warning
from kl_site_server.calc import (
    CachedDataTooOldError, calculate_indicators_full, calculate_indicators_last,
    calculate_indicators_partial,
)
from kl_site_server.db import (
    CalculatedDataLookup, StoreCalculatedDataArgs, get_calculated_data_from_db, get_history_data_from_db_full,
    get_history_data_from_db_limit_count,
    store_calculated_to_db,
)
from kl_site_server.model import PxDataCache, TouchancePxRequestParams
from kl_site_server.utils import MAX_PERIOD, MAX_PERIOD_NO_EMA
from tcoreapi_mq.message import HistoryInterval, PxHistoryDataEntry
from tcoreapi_mq.model import SymbolBaseType


class PeriodIntervalInfo(NamedTuple):
    period_min: int
    interval: HistoryInterval
    max_period_num: int


FuncGetHistoryData: TypeAlias = Callable[[tuple[SymbolBaseType, HistoryInterval], int], list[PxHistoryDataEntry]]

FuncGetCalculatedDataLookup: TypeAlias = Callable[[list[str], list[int]], CalculatedDataLookup]

FuncSingleCalcDataUpdate: TypeAlias = Callable[
    [SymbolBaseType, PeriodIntervalInfo, CalculatedDataLookup, DataCache],
    StoreCalculatedDataArgs | None
]


class CalculatedDataManager:
    def __init__(self, px_data_cache: PxDataCache, px_request_params: dict[str, TouchancePxRequestParams]):
        self._px_data_cache: PxDataCache = px_data_cache
        self._px_request_params: dict[str, TouchancePxRequestParams] = px_request_params

        self._update_calculated_data_lock: Lock = Lock()
        self._update_calculated_data_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=8)

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

    def _calc_data_update_single_new_bar(
        self,
        symbol_obj: SymbolBaseType,
        interval_info: PeriodIntervalInfo,
        calculated_data_lookup: CalculatedDataLookup,
        history_data_cache: DataCache,
    ) -> StoreCalculatedDataArgs | None:
        if not self._px_data_cache.is_all_ready_of_intervals(
                [interval_info.interval],
                symbol_obj.symbol_complete
        ):
            return None

        cached_calculated_data = calculated_data_lookup.get_calculated_data(
            symbol_obj.symbol_complete, interval_info.period_min
        )

        cached_calculated_df = DataFrame(cached_calculated_data) if cached_calculated_data else None
        data_recs = history_data_cache.get_value(
            (symbol_obj, interval_info.interval),
            payload=interval_info.max_period_num
        )

        if cached_calculated_df is None or not data_recs:
            print_warning(
                "[TC Client] No history or cached calculated data available "
                f"for [bold]{symbol_obj.security}[/bold]@{interval_info.period_min}"
            )
            return None

        print_log(
            "[TC Client] Calculating indicators of "
            f"[yellow]{symbol_obj.security}@{interval_info.period_min}[/yellow] on partial data"
        )

        try:
            calculated_df = calculate_indicators_partial(
                interval_info.period_min,
                data_recs,
                cached_calculated_df
            )
        except CachedDataTooOldError:
            calculated_df = calculate_indicators_full(interval_info.period_min, data_recs)

        return StoreCalculatedDataArgs(symbol_obj, interval_info.period_min, calculated_df, True)

    def _calc_data_update_single_common(
        self,
        symbol_obj: SymbolBaseType,
        interval_info: PeriodIntervalInfo,
        calculated_data_lookup: CalculatedDataLookup,
        history_data_cache: DataCache,
    ) -> StoreCalculatedDataArgs | None:
        symbol_complete = symbol_obj.symbol_complete

        if not self._px_data_cache.is_all_ready_of_intervals([interval_info.interval], symbol_complete):
            return None

        cached_calculated_data = calculated_data_lookup.get_calculated_data(
            symbol_complete, interval_info.period_min
        )
        cached_calculated_df = DataFrame(cached_calculated_data) if cached_calculated_data else None

        calculated_df = None
        # Calculated data might not have newer bars
        if cached_calculated_df is not None:
            calculated_df = calculate_indicators_last(interval_info.period_min, cached_calculated_df)
        elif data_recs := history_data_cache.get_value(
                (symbol_obj, interval_info.interval),
                payload=interval_info.max_period_num
        ):
            print_log(
                "[TC Client] Calculating indicators of "
                f"[yellow]{symbol_obj.security}@{interval_info.period_min}[/yellow] on all data"
            )
            calculated_df = calculate_indicators_full(interval_info.period_min, data_recs)

        if calculated_df is None:
            print_warning(
                "[TC Client] No history or cached calculated data available "
                f"for [bold]{symbol_obj.security}[/bold]@{interval_info.period_min}"
            )
            return None

        return StoreCalculatedDataArgs(
            symbol_obj, interval_info.period_min, calculated_df, cached_calculated_data is None
        )

    def _calc_data_update(
        self,
        fn_get_history_data: FuncGetHistoryData,
        fn_get_calculated_data_lookup: FuncGetCalculatedDataLookup,
        fn_calc_update_single: FuncSingleCalcDataUpdate,
        symbols: Iterable[SymbolBaseType]
    ) -> None:
        interval_info_list = self._get_params_interval_info()
        store_calculated_args: list[StoreCalculatedDataArgs] = []
        history_data_cache: DataCache = DataCache(fn_get_history_data)
        calculated_data_lookup = fn_get_calculated_data_lookup(
            [symbol.symbol_complete for symbol in symbols],
            [interval_info.period_min for interval_info in interval_info_list]
        )

        with ThreadPoolExecutor() as executor:
            for future in as_completed([
                executor.submit(
                    fn_calc_update_single,
                    symbol_obj, interval_info, calculated_data_lookup, history_data_cache
                )
                for symbol_obj, interval_info in product(symbols, interval_info_list)
            ]):
                if calc_args := future.result():
                    store_calculated_args.append(calc_args)

        store_calculated_to_db(store_calculated_args)

    def _calc_data_update_lockable(
        self,
        fn_get_history_data: FuncGetHistoryData,
        fn_get_calculated_data_lookup: FuncGetCalculatedDataLookup,
        fn_calc_update_single: FuncSingleCalcDataUpdate,
        symbols: Iterable[SymbolBaseType], *,
        skip_if_locked: bool,
        threaded: bool,
    ) -> None:
        if skip_if_locked and self._update_calculated_data_lock.locked():
            return

        with self._update_calculated_data_lock:
            if threaded:
                self._update_calculated_data_executor.submit(
                    self._calc_data_update,
                    fn_get_history_data, fn_get_calculated_data_lookup,
                    fn_calc_update_single, symbols
                )
            else:
                self._calc_data_update(
                    fn_get_history_data, fn_get_calculated_data_lookup,
                    fn_calc_update_single, symbols
                )

    def update_calc_data_new_bar(self) -> None:
        def get_history_data(
            key: tuple[SymbolBaseType, HistoryInterval], max_period_num: int
        ) -> list[PxHistoryDataEntry]:
            symbol, interval = key

            data = get_history_data_from_db_limit_count(
                symbol.symbol_complete,
                interval,
                max_period_num * MAX_PERIOD_NO_EMA
            ).data

            return data

        def get_calculated_data_lookup(
            symbol_complete_list: list[str],
            period_mins: list[int],
        ) -> CalculatedDataLookup:
            return get_calculated_data_from_db(
                symbol_complete_list, period_mins,
                count=MAX_PERIOD_NO_EMA
            )

        self._calc_data_update(
            get_history_data,
            get_calculated_data_lookup,
            self._calc_data_update_single_new_bar,
            self._px_data_cache.symbol_obj_in_use
        )

    def update_calc_data_last(self) -> None:
        def get_history_data(key: tuple[SymbolBaseType, HistoryInterval], _: int) -> list[PxHistoryDataEntry]:
            symbol, interval = key

            symbol_complete = symbol.symbol_complete

            last_entry = PxHistoryDataEntry.from_bar_data_dict(
                symbol_complete,
                interval,
                self._px_data_cache.get_cache_entry_of_interval(interval, symbol_complete).data_last_bar
            )

            return get_history_data_from_db_full(symbol_complete, interval).update_latest(last_entry).data

        def get_calculated_data_lookup(
            symbol_complete_list: list[str],
            period_mins: list[int],
        ) -> CalculatedDataLookup:
            return get_calculated_data_from_db(
                symbol_complete_list, period_mins,
                count=MAX_PERIOD
            )

        self._calc_data_update_lockable(
            get_history_data,
            get_calculated_data_lookup,
            self._calc_data_update_single_common,
            self._px_data_cache.symbol_obj_in_use,
            threaded=True,
            skip_if_locked=True,
        )

    def update_calc_data_full(self, symbol_obj: SymbolBaseType) -> None:
        def get_history_data(key: tuple[SymbolBaseType, HistoryInterval], _: int) -> list[PxHistoryDataEntry]:
            symbol, interval = key

            return get_history_data_from_db_full(symbol.symbol_complete, interval).data

        def get_calculated_data_lookup(
            _: list[str],
            __: list[int],
        ) -> CalculatedDataLookup:
            return CalculatedDataLookup()

        print_log(f"[TC Client] [blue]Started data re-calculation of [yellow]{symbol_obj.security}[/yellow][/blue]")
        self._calc_data_update_lockable(
            get_history_data,
            get_calculated_data_lookup,
            self._calc_data_update_single_common,
            {symbol_obj},
            threaded=False,
            skip_if_locked=False,
        )
        print_log(f"[TC Client] [blue]Completed data re-calculation of [yellow]{symbol_obj.security}[/yellow][/blue]")
