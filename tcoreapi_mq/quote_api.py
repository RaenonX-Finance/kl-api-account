from collections import defaultdict
from datetime import datetime
from threading import Lock

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


class QuoteAPI(TCoreZMQ):
    def __init__(self):
        super().__init__(SYS_APP_ID, SYS_SERVICE_KEY)

        self._info: dict[str, QueryInstrumentProduct] = {}
        self._subscribing_realtime: set[str] = set()
        self.history_data_lock_dict: defaultdict[str, Lock] = defaultdict(Lock)

    def get_symbol_info(self, symbol_obj: SymbolBaseType) -> QueryInstrumentProduct:
        if ret := self._info.get(symbol_obj.symbol_complete):
            return ret

        self._info[symbol_obj.symbol_complete] = self.query_instrument_info(symbol_obj).info_product

        return self._info[symbol_obj.symbol_complete]

    def subscribe_realtime(self, symbol: SymbolBaseType) -> SubscribeRealtimeMessage:
        print_log(f"[TC Quote] Subscribing realtime data of [yellow]{symbol.symbol_complete}[/yellow]")

        self._subscribing_realtime.add(symbol.symbol_complete)

        with self.lock:
            req = SubscribeRealtimeRequest(session_key=self.session_key, symbol=symbol)
            self.socket.send_string(req.to_message())

            return SubscribeRealtimeMessage(message=self.socket.get_message())

    def is_subscribing_realtime(self, symbol_complete: str) -> bool:
        return symbol_complete in self._subscribing_realtime

    def unsubscribe_realtime(self, symbol_complete: str) -> UnsubscribeRealtimeMessage:
        print_log(f"[TC Quote] Unsubscribing realtime data from [yellow]{symbol_complete}[/yellow]")

        if self.is_subscribing_realtime(symbol_complete):
            self._subscribing_realtime.remove(symbol_complete)

        with self.lock:
            req = UnsubscribeRealtimeRequest(session_key=self.session_key, symbol_complete=symbol_complete)
            self.socket.send_string(req.to_message())

            return UnsubscribeRealtimeMessage(message=self.socket.get_message())

    def get_history(
        self,
        symbol: SymbolBaseType,
        interval: HistoryInterval,
        start: datetime,
        end: datetime,
    ) -> SubscribePxHistoryMessage | None:
        """Get the history data. Does NOT automatically update upon new candlestick/data generation."""
        self.history_data_lock_dict[symbol.symbol_complete].acquire()
        print_log(
            f"[TC Quote] Request history data of "
            f"[yellow]{symbol.security}[/yellow] at [yellow]{interval}[/yellow] "
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

                self.socket.send_string(req.to_message())
            except ValueError:
                print_warning(f"[TC Quote] Omit history data request (Start = End, {start} ~ {end})")
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
        interval = handshake.data_type
        start_time_str = handshake.start_time_str
        end_time_str = handshake.end_time_str

        if self.history_data_lock_dict[symbol_complete].locked():
            # Request from other session could trigger this, therefore using `locked()` to guard
            self.history_data_lock_dict[symbol_complete].release()
        print_log(
            f"[TC Quote] History data fetching completed for [yellow]{symbol_complete}[/yellow] "
            f"at [yellow]{interval}[/yellow] starting from {start_time_str} to {end_time_str}"
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
