import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Callable

from kl_site_common.const import DATA_PX_REFETCH_BACKWARD_HOUR, DATA_PX_REFETCH_INTERVAL_SEC
from kl_site_common.utils import print_log, print_warning
from kl_site_server.db import is_market_closed
from kl_site_server.model import PxDataCache, TouchancePxRequestParams
from tcoreapi_mq.message import HistoryInterval
from tcoreapi_mq.model import SymbolBaseType

FuncNonBlockRequestHistory = Callable[[SymbolBaseType, HistoryInterval, datetime, datetime], None]


class HistoryDataRefetcher:
    def __init__(
        self, px_data_cache: PxDataCache, px_request_params: dict[str, TouchancePxRequestParams],
        func_req_history: FuncNonBlockRequestHistory
    ):
        self._px_data_cache: PxDataCache = px_data_cache
        self._px_request_params: dict[str, TouchancePxRequestParams] = px_request_params
        self._func_req_history: FuncNonBlockRequestHistory = func_req_history

    def start(self):
        Thread(target=self._history_data_refetcher).start()

    def _history_data_refetcher(self):
        while True:
            time.sleep(DATA_PX_REFETCH_INTERVAL_SEC)
            if not self._px_data_cache.is_all_px_data_ready():
                print_warning("Skipped re-fetching history px data - not all px data are ready")
                continue

            # Create list to avoid size change during iteration error
            for params in list(self._px_request_params.values()):
                if is_market_closed(params.symbol_obj.security):
                    print_log(
                        f"Skipped re-fetching history of "
                        f"[yellow]{params.symbol_obj.security}[/] - [red]market closed[/]"
                    )
                    continue

                start = datetime.utcnow() - timedelta(hours=DATA_PX_REFETCH_BACKWARD_HOUR)
                end = datetime.utcnow() + timedelta(minutes=2)

                print_log(f"Re-fetching history of [yellow]{params.symbol_obj.security}[/]")

                if params.period_mins:
                    self._func_req_history(params.symbol_obj, "1K", start, end)

                if params.period_days:
                    self._func_req_history(params.symbol_obj, "DK", start, end)
