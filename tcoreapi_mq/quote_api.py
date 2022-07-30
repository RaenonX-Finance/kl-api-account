from datetime import datetime

from kl_site_common.const import SYS_APP_ID, SYS_SERVICE_KEY
from kl_site_common.utils import print_log, print_warning
from .core import TCoreZMQ
from .message import (
    CompletePxHistoryMessage, CompletePxHistoryRequest, GetPxHistoryMessage, GetPxHistoryRequest, HistoryInterval,
    QueryInstrumentProduct, SubscribePxHistoryMessage, SubscribePxHistoryRequest,
    SubscribeRealtimeMessage, SubscribeRealtimeRequest, UnsubscribeRealtimeMessage, UnsubscribeRealtimeRequest,
)
from .model import SymbolBaseType


class QuoteAPI(TCoreZMQ):
    def __init__(self):
        super().__init__(SYS_APP_ID, SYS_SERVICE_KEY)

        self._info: dict[str, QueryInstrumentProduct] = {}
        self._subscribing_realtime: set[str] = set()

    def register_symbol_info(self, symbol_obj: SymbolBaseType) -> None:
        if symbol_obj.symbol_complete in self._info:
            return

        msg = self.query_instrument_info(symbol_obj)

        self._info[msg.symbol_obj.symbol_complete] = msg.info_product

    def get_instrument_info_by_symbol(self, symbol_obj: SymbolBaseType) -> QueryInstrumentProduct:
        key = symbol_obj.symbol_complete

        if key not in self._info:
            raise ValueError(
                f"Symbol `{symbol_obj}` not yet registered. "
                f"Run `register_symbol_info()`, `subscribe_realtime()`, or `subscribe_history()` first."
            )

        return self._info[key]

    def subscribe_realtime(self, symbol: SymbolBaseType) -> SubscribeRealtimeMessage:
        print_log(f"[TC Quote] Subscribing realtime data of [yellow]{symbol.symbol_complete}[/yellow]")

        self.register_symbol_info(symbol)
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
        print_log(
            f"[TC Quote] Requesting history data of "
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
                print_warning(f"[TC Quote] Start time = end time, omitting request ({start} ~ {end})")
                return None

            return SubscribePxHistoryMessage(message=self.socket.get_message())

    def get_paged_history(
        self, symbol_complete: str, interval: HistoryInterval,
        start: str, end: str, query_idx: int = 0
    ) -> GetPxHistoryMessage:
        """
        Usually this is called after receiving the subscription data after calling ``subscribe_history()``.

        Parameters originated from the subscription data of ``subscribe_history()``.
        """
        with self.lock:
            req = GetPxHistoryRequest(
                session_key=self.session_key,
                symbol_complete=symbol_complete,
                interval=interval,
                start_time_str=start,
                end_time_str=end,
                query_idx=query_idx
            )
            self.socket.send_string(req.to_message())

            return GetPxHistoryMessage(message=self.socket.get_message())

    def complete_get_history(self, symbol_complete: str, interval: HistoryInterval, start: str, end: str):
        print_log(
            f"[TC Quote] History data fetching completed for [yellow]{symbol_complete}[/yellow] "
            f"at [yellow]{interval}[/yellow] starting from {start} to {end}"
        )
        with self.lock:
            req = CompletePxHistoryRequest(
                session_key=self.session_key,
                symbol_complete=symbol_complete,
                interval=interval,
                start_time_str=start,
                end_time_str=end
            )
            self.socket.send_string(req.to_message())

            msg = self.socket.get_message()

            return CompletePxHistoryMessage(message=msg)
