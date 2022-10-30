from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from threading import Lock
from typing import Callable, Iterable, NamedTuple, TypeAlias

from pandas import DataFrame

from kl_site_common.utils import DataCache, print_log, print_warning
from kl_site_server.calc import (
    CachedDataTooOldError, calc_set_epoch_index, calculate_indicators_full, calculate_indicators_last,
    calculate_indicators_partial,
)
from kl_site_server.db import (
    CalculatedDataLookup, StoreCalculatedDataArgs, get_calculated_data_from_db, get_history_data_from_db_full,
    get_history_data_from_db_limit_count,
    store_calculated_to_db,
)
from kl_site_server.model import BarDataDict, PxDataCache, TouchancePxRequestParams
from kl_site_server.utils import MAX_PERIOD, MAX_PERIOD_NO_EMA
from tcoreapi_mq.message import HistoryInterval, PxHistoryDataEntry
from tcoreapi_mq.model import SymbolBaseType


class PeriodIntervalInfo(NamedTuple):
    period_min: int
    interval: HistoryInterval
    max_period_num: int


FuncGetHistoryData: TypeAlias = Callable[[tuple[SymbolBaseType, HistoryInterval], int], DataFrame]

FuncGetCalculatedDataLookup: TypeAlias = Callable[[list[str], list[int]], CalculatedDataLookup]

FuncSingleCalcDataUpdate: TypeAlias = Callable[
    [SymbolBaseType, PeriodIntervalInfo, CalculatedDataLookup, DataCache],
    StoreCalculatedDataArgs | None
]


class CalculatedDataManager:
    def __init__(self, px_data_cache: PxDataCache):
        self._px_data_cache: PxDataCache = px_data_cache

        self._update_calculated_data_lock: Lock = Lock()
        self._update_calculated_data_executor: ThreadPoolExecutor = ThreadPoolExecutor()

    @staticmethod
    def _get_params_interval_info(params_list: Iterable[TouchancePxRequestParams]) -> set[PeriodIntervalInfo]:
        period_min_set = set()

        max_period_num: dict[HistoryInterval, int] = {
            "1K": max(
                period_min for params in params_list
                for period_min in params.period_mins
            ),
            "DK": max(
                period_day for params in params_list
                for period_day in params.period_days
            ),
        }

        for params in params_list:
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
        symbol_complete = symbol_obj.symbol_complete
        period_min = interval_info.period_min

        if not self._px_data_cache.is_all_ready_of_intervals([interval_info.interval], symbol_complete):
            return None

        cached_calculated_data = calculated_data_lookup.get_calculated_data(symbol_complete, period_min)

        cached_calculated_df = DataFrame(cached_calculated_data) if cached_calculated_data else None
        df_interval = history_data_cache.get_value(
            (symbol_obj, interval_info.interval),
            payload=interval_info.max_period_num
        )

        if cached_calculated_df is None or df_interval is None:
            print_warning(
                f"No history or cached calculated data available for [bold]{symbol_obj.security}[/]@{period_min}"
            )
            return None

        print_log(f"Calculating indicators of [yellow]{symbol_obj.security}@{period_min}[/] on partial data")

        try:
            calculated_df = calculate_indicators_partial(
                symbol_obj.security,
                period_min,
                df_interval,
                cached_calculated_df
            )
        except CachedDataTooOldError:
            calculated_df = calculate_indicators_full(period_min, df_interval)

        return StoreCalculatedDataArgs(symbol_obj, period_min, calculated_df, True)

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
        else:
            df_interval = history_data_cache.get_value(
                (symbol_obj, interval_info.interval),
                payload=interval_info.max_period_num
            )

            if df_interval is not None:
                print_log(
                    "Calculating indicators of "
                    f"[yellow]{symbol_obj.security}@{interval_info.period_min}[/] on all data"
                )
                calculated_df = calculate_indicators_full(interval_info.period_min, df_interval)

        if calculated_df is None:
            print_warning(
                "No history or cached calculated data available "
                f"for [bold]{symbol_obj.security}[/]@{interval_info.period_min}"
            )
            return None

        return StoreCalculatedDataArgs(
            symbol_obj, interval_info.period_min, calculated_df, cached_calculated_data is None
        )

    def _calc_data_update_inner(
        self,
        fn_get_history_data: FuncGetHistoryData,
        fn_get_calculated_data_lookup: FuncGetCalculatedDataLookup,
        fn_calc_update_single: FuncSingleCalcDataUpdate,
        symbols: Iterable[SymbolBaseType],
        params_list: Iterable[TouchancePxRequestParams],
        skip_if_locked: bool,
    ) -> None:
        if not params_list:
            print_warning("No px request params available for updating calc data")
            return
        elif skip_if_locked and self._update_calculated_data_lock.locked():
            print_log("Skipped calculating px data - lock acquired")
            return

        with self._update_calculated_data_lock:
            interval_info_set = self._get_params_interval_info(params_list)
            store_calculated_args: list[StoreCalculatedDataArgs] = []
            history_data_cache: DataCache = DataCache(fn_get_history_data)
            calculated_data_lookup = fn_get_calculated_data_lookup(
                [symbol.symbol_complete for symbol in symbols],
                [interval_info.period_min for interval_info in interval_info_set],
            )

            with ThreadPoolExecutor() as executor:
                for future in as_completed([
                    executor.submit(
                        fn_calc_update_single,
                        symbol_obj, interval_info, calculated_data_lookup, history_data_cache
                    )
                    for symbol_obj, interval_info in product(symbols, interval_info_set)
                ]):
                    if calc_args := future.result():
                        store_calculated_args.append(calc_args)

            store_calculated_to_db(store_calculated_args)

    def _calc_data_update(
        self,
        fn_get_history_data: FuncGetHistoryData,
        fn_get_calculated_data_lookup: FuncGetCalculatedDataLookup,
        fn_calc_update_single: FuncSingleCalcDataUpdate,
        symbols: Iterable[SymbolBaseType],
        params_list: Iterable[TouchancePxRequestParams],
        *,
        skip_if_locked: bool,
        threaded: bool,
    ) -> None:
        if threaded:
            self._update_calculated_data_executor.submit(
                self._calc_data_update_inner,
                fn_get_history_data, fn_get_calculated_data_lookup,
                fn_calc_update_single, symbols, params_list, skip_if_locked
            )
        else:
            self._calc_data_update_inner(
                fn_get_history_data, fn_get_calculated_data_lookup,
                fn_calc_update_single, symbols, params_list, skip_if_locked
            )

    def update_calc_data_new_bar(self, params_list: Iterable[TouchancePxRequestParams]) -> None:
        def get_history_data(key: tuple[SymbolBaseType, HistoryInterval], max_period_num: int) -> DataFrame:
            symbol, interval = key

            df = DataFrame(get_history_data_from_db_limit_count(
                symbol.symbol_complete,
                interval,
                max_period_num * MAX_PERIOD_NO_EMA
            ).data)

            calc_set_epoch_index(df)

            return df

        def get_calculated_data_lookup(
            symbol_complete_list: list[str],
            period_mins: list[int],
        ) -> CalculatedDataLookup:
            # Splitting period mins to reduce to number of total bars needed to fetch
            # This should be removed after `get_calculated_data_from_db()`
            # optimization for guaranteeing the bars count
            data_lt_1440 = get_calculated_data_from_db(
                symbol_complete_list, [period_min for period_min in period_mins if period_min < 1440],
            )
            data_gte_1440 = get_calculated_data_from_db(
                symbol_complete_list, [period_min for period_min in period_mins if period_min >= 1440],
            )

            return data_lt_1440.merge(data_gte_1440)

        self._calc_data_update(
            get_history_data,
            get_calculated_data_lookup,
            self._calc_data_update_single_new_bar,
            self._px_data_cache.symbol_obj_in_use,
            params_list,
            skip_if_locked=False,
            threaded=False,
        )

    def update_calc_data_last(self, params_list: Iterable[TouchancePxRequestParams]) -> None:
        def get_history_data(key: tuple[SymbolBaseType, HistoryInterval], _: int) -> DataFrame:
            symbol, interval = key

            symbol_complete = symbol.symbol_complete

            last_entry = PxHistoryDataEntry.from_bar_data_dict(
                symbol_complete,
                interval,
                self._px_data_cache.get_cache_entry_of_interval(interval, symbol_complete).data_last_bar
            )

            df = DataFrame(get_history_data_from_db_full(symbol_complete, interval).update_latest(last_entry).data)

            calc_set_epoch_index(df)

            return df

        def get_calculated_data_lookup(
            symbol_complete_list: list[str],
            period_mins: list[int],
        ) -> CalculatedDataLookup:
            last_bar_dict: dict[str, BarDataDict] = {
                symbol_complete: self._px_data_cache.get_cache_entry_of_interval("1K", symbol_complete).data_last_bar
                for symbol_complete in symbol_complete_list
            }

            return get_calculated_data_from_db(
                symbol_complete_list, period_mins,
                count=MAX_PERIOD
            ).update_last_bar(last_bar_dict)

        self._calc_data_update(
            get_history_data,
            get_calculated_data_lookup,
            self._calc_data_update_single_common,
            self._px_data_cache.symbol_obj_in_use,
            params_list,
            threaded=True,
            skip_if_locked=True,
        )

    def update_calc_data_full(self, symbol_obj: SymbolBaseType, params_list: list[TouchancePxRequestParams]) -> None:
        def get_history_data(key: tuple[SymbolBaseType, HistoryInterval], _: int) -> DataFrame:
            symbol, interval = key

            df = DataFrame(get_history_data_from_db_full(symbol.symbol_complete, interval).data)

            calc_set_epoch_index(df)

            return df

        def get_calculated_data_lookup(
            _: list[str],
            __: list[int],
        ) -> CalculatedDataLookup:
            return CalculatedDataLookup()

        print_log(f"[blue]Started data re-calculation of [yellow]{symbol_obj.security}[/]")
        self._calc_data_update(
            get_history_data,
            get_calculated_data_lookup,
            self._calc_data_update_single_common,
            {symbol_obj},
            params_list,
            threaded=False,
            skip_if_locked=False,
        )
        print_log(f"[blue]Completed data re-calculation of [yellow]{symbol_obj.security}[/]")
