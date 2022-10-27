from collections import defaultdict
from datetime import datetime
from threading import Lock
from typing import TypeAlias

from kl_site_common.const import SYS_APP_ID, SYS_SERVICE_KEY
from kl_site_common.utils import print_log, print_warning
from .core import TCoreZMQ
from .message import (
    CompletePxHistoryMessage, CompletePxHistoryRequest, GetPxHistoryMessage, GetPxHistoryRequest, HistoryDataHandshake,
    HistoryInterval,
    QueryInstrumentProduct, SubscribePxHistoryMessage, SubscribePxHistoryRequest,
    SubscribeRealtimeMessage, SubscribeRealtimeRequest, UnsubscribeRealtimeMessage, UnsubscribeRealtimeRequest,
)
from .model import SymbolBaseType

UnsubHistQueueKey: TypeAlias = tuple[str, str, str]  # Symbol complete, start ts str, end ts str


class QuoteAPI(TCoreZMQ):
    def __init__(self):
        super().__init__(SYS_APP_ID, SYS_SERVICE_KEY)

        self._info: dict[str, QueryInstrumentProduct] = {}
        self._subscribing_realtime: set[str] = set()
        self._unsub_hist_after_complete: set[UnsubHistQueueKey] = set()
        self.history_data_lock_dict: defaultdict[str, Lock] = defaultdict(Lock)

    def get_symbol_info(self, symbol_obj: SymbolBaseType) -> QueryInstrumentProduct:
        if ret := self._info.get(symbol_obj.symbol_complete):
            return ret

        self._info[symbol_obj.symbol_complete] = self.query_instrument_info(symbol_obj).info_product

        return self._info[symbol_obj.symbol_complete]

    def subscribe_realtime(self, symbol: SymbolBaseType) -> SubscribeRealtimeMessage:
        print_log(f"Subscribing realtime data of [yellow]{symbol.security}[/]")

        self._subscribing_realtime.add(symbol.symbol_complete)

        with self.lock:
            req = SubscribeRealtimeRequest(session_key=self.session_key, symbol=symbol)
            self.socket.send_string(req.to_message())

            return SubscribeRealtimeMessage(message=self.socket.get_message())

    def is_subscribing_realtime(self, symbol_complete: str) -> bool:
        return symbol_complete in self._subscribing_realtime

    def unsubscribe_realtime(self, symbol_complete: str) -> UnsubscribeRealtimeMessage:
        print_log(f"Unsubscribing realtime data from [yellow]{symbol_complete}[/]")

        if self.is_subscribing_realtime(symbol_complete):
            self._subscribing_realtime.remove(symbol_complete)

        with self.lock:
            req = UnsubscribeRealtimeRequest(session_key=self.session_key, symbol_complete=symbol_complete)
            self.socket.send_string(req.to_message())

            return UnsubscribeRealtimeMessage(message=self.socket.get_message())

    @staticmethod
    def _make_unsub_queue_key(symbol_complete: str, start_ts_str: str, end_ts_str: str) -> UnsubHistQueueKey:
        return symbol_complete, start_ts_str, end_ts_str

    def get_history(
        self,
        symbol: SymbolBaseType,
        interval: HistoryInterval,
        start: datetime,
        end: datetime, *,
        ignore_lock: bool = False,
        unsub_after_complete: bool = False,
    ) -> SubscribePxHistoryMessage | None:
        """Get the history data. Does NOT automatically update upon new candlestick/data generation."""
        if not ignore_lock:
            self.history_data_lock_dict[symbol.symbol_complete].acquire()
        print_log(
            f"Request history data of [yellow]{symbol.security}[/] at [yellow]{interval}[/] "
            f"starting from {start} to {end}"
        )

        with self.lock:
            try:
                req = SubscribePxHistoryRequest(
                    session_key=self.session_key,
                    symbol=symbol,
                    interval=interval,
                    start_time=start,
                    end_time=end
                )

                if unsub_after_complete:
                    self._unsub_hist_after_complete.add(self._make_unsub_queue_key(
                        symbol.symbol_complete, req.start_ts_str, req.end_ts_str
                    ))

                self.socket.send_string(req.to_message())
            except ValueError:
                print_warning(f"Omit history data request (Start = End, {start} ~ {end})")
                return None

            return SubscribePxHistoryMessage(message=self.socket.get_message())

    def get_paged_history(self, handshake: HistoryDataHandshake, query_idx: int = 0) -> GetPxHistoryMessage:
        """
        Usually this is called after receiving the subscription data after calling ``subscribe_history()``.

        Parameters originated from the subscription data of ``subscribe_history()``.
        """
        with self.lock:
            req = GetPxHistoryRequest(
                session_key=self.session_key,
                symbol_complete=handshake.symbol_complete,
                interval=handshake.data_type,
                start_time_str=handshake.start_time_str,
                end_time_str=handshake.end_time_str,
                query_idx=query_idx
            )
            self.socket.send_string(req.to_message())

            return GetPxHistoryMessage(message=self.socket.get_message())

    def complete_get_history(self, handshake: HistoryDataHandshake):
        symbol_complete = handshake.symbol_complete

        if self.history_data_lock_dict[symbol_complete].locked():
            # Request from other session could trigger this, therefore using `locked()` to guard
            self.history_data_lock_dict[symbol_complete].release()

        unsub_key = self._make_unsub_queue_key(symbol_complete, handshake.start_time_str, handshake.end_time_str)
        if unsub_key in self._unsub_hist_after_complete:
            self.unsubscribe_history(handshake, do_not_execute_complete=True)

    def unsubscribe_history(self, handshake: HistoryDataHandshake, *, do_not_execute_complete: bool = False):
        symbol_complete = handshake.symbol_complete
        interval = handshake.data_type
        start_time_str = handshake.start_time_str
        end_time_str = handshake.end_time_str

        if not do_not_execute_complete:
            self.complete_get_history(handshake)

        print_log(
            f"Unsubscribing history data of [yellow]{symbol_complete}[/] "
            f"at [yellow]{interval}[/] starting from {start_time_str} to {end_time_str}"
        )

        with self.lock:
            req = CompletePxHistoryRequest(
                session_key=self.session_key,
                symbol_complete=symbol_complete,
                interval=interval,
                start_time_str=handshake.start_time_str,
                end_time_str=handshake.end_time_str
            )
            self.socket.send_string(req.to_message())

            msg = self.socket.get_message()

            return CompletePxHistoryMessage(message=msg)
