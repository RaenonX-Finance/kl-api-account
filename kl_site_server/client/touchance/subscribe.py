from datetime import datetime, timedelta
from typing import Callable

from kl_site_common.const import DATA_PX_REFETCH_BACKWARD_HOUR
from kl_site_common.utils import print_error, print_log
from kl_site_server.model import PxDataCache, TouchancePxRequestParams
from tcoreapi_mq.message import HistoryInterval
from tcoreapi_mq.model import SymbolBaseType

FuncSubscribeHistory = Callable[[SymbolBaseType, HistoryInterval, datetime, datetime], None]


class HistoryDataSubscriber:
    def __init__(
        self, px_data_cache: PxDataCache, px_request_params: dict[str, TouchancePxRequestParams],
        func_req_history: FuncSubscribeHistory
    ):
        self._px_data_cache: PxDataCache = px_data_cache
        self._px_request_params: dict[str, TouchancePxRequestParams] = px_request_params
        self._func_req_history: FuncSubscribeHistory = func_req_history

    def start(self):
        if not self._px_data_cache.is_all_px_data_ready():
            print_error("Skipped subscribing history px data - not all px data are ready")

        # Create list to avoid size change during iteration error
        for params in list(self._px_request_params.values()):
            start = datetime.utcnow() - timedelta(hours=DATA_PX_REFETCH_BACKWARD_HOUR)
            end = datetime.utcnow() + timedelta(minutes=2)

            print_log(f"Subscribing history of [yellow]{params.symbol_obj.security}[/] from {start}")

            if params.period_mins:
                self._func_req_history(params.symbol_obj, "1K", start, end)

            if params.period_days:
                self._func_req_history(params.symbol_obj, "DK", start, end)
