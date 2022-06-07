from datetime import datetime

from .core import TCoreZMQ
from .message import (
    GetPxHistoryMessage, GetPxHistoryRequest, HistoryInterval, SubscribePxHistoryMessage, SubscribePxHistoryRequest,
    SubscribeRealtimeMessage, SubscribeRealtimeRequest, UnsubscribeRealtimeMessage, UnsubscribeRealtimeRequest,
)
from .model import SymbolBaseType
from .utils import print_log


class QuoteAPI(TCoreZMQ):
    def subscribe_realtime(self, symbol: SymbolBaseType) -> SubscribeRealtimeMessage:
        print_log(f"[Quote] Subscribing realtime data of [yellow]{symbol.symbol_name}[/yellow]")

        with self.lock:
            req = SubscribeRealtimeRequest(session_key=self.session_key, symbol=symbol)
            self.socket.send_string(req.to_message())

            return SubscribeRealtimeMessage(message=self.socket.get_message())

    def unsubscribe_realtime(self, symbol: SymbolBaseType) -> UnsubscribeRealtimeMessage:
        print_log(f"[Quote] Unsubscribing realtime data from [yellow]{symbol.symbol_name}[/yellow]")

        with self.lock:
            req = UnsubscribeRealtimeRequest(session_key=self.session_key, symbol=symbol)
            self.socket.send_string(req.to_message())

            return UnsubscribeRealtimeMessage(message=self.socket.get_message())

    def subscribe_history(
            self,
            symbol: SymbolBaseType, interval: HistoryInterval,
            start: datetime, end: datetime,
    ) -> SubscribePxHistoryMessage:
        print_log(
            f"[Quote] Subscribing historical data of "
            f"[yellow]{symbol.symbol_name}[/yellow] at [yellow]{interval}[/yellow]"
        )
        print_log(f"[Quote] Historical data starts from {start} to {end}")

        with self.lock:
            req = SubscribePxHistoryRequest(
                session_key=self.session_key,
                symbol=symbol,
                interval=interval,
                start_time=start,
                end_time=end
            )
            self.socket.send_string(req.to_message())

            return SubscribePxHistoryMessage(message=self.socket.get_message())

    def get_paged_history(
            self, symbol: str, interval: HistoryInterval,
            start: str, end: str, query_idx: int = 0
    ) -> GetPxHistoryMessage:
        """
        Usually this is called after receiving the subscription data after calling ``subscribe_history()``.

        Parameters originated from the subscription data of ``subscribe_history()``.
        """
        print_log(
            f"[Quote] Getting paged historical data of [yellow]{symbol}[/yellow] "
            f"at [yellow]{interval}[/yellow] (#{query_idx})"
        )
        print_log(f"[Quote] Paged historical data starts from {start} to {end}")

        with self.lock:
            req = GetPxHistoryRequest(
                session_key=self.session_key,
                symbol_name=symbol,
                interval=interval,
                start_time_str=start,
                end_time_str=end,
                query_idx=query_idx
            )
            self.socket.send_string(req.to_message())

            return GetPxHistoryMessage(message=self.socket.get_message())
