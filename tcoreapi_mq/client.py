import threading
from abc import ABC, abstractmethod

from kl_site_common.utils import print_error, print_warning
from kl_site_common.const import DATA_TIMEOUT_SEC, SYS_PORT_QUOTE

from .message import CommonData, HistoryData, HistoryDataHandshake, RealtimeData
from .quote_api import QuoteAPI
from .utils import create_subscription_receiver_socket


class TouchanceApiClient(QuoteAPI, ABC):
    def start(self):
        login_result = self.connect(SYS_PORT_QUOTE)

        if not login_result.success:
            raise RuntimeError("Px quoting connection failed")

        threading.Thread(target=self._quote_subscription_loop, args=(login_result.sub_port,)).start()

    @abstractmethod
    def on_received_realtime_data(self, data: RealtimeData) -> None:
        raise NotImplementedError()

    @abstractmethod
    def on_received_history_data(self, data: HistoryData) -> None:
        """
        Method to be called after calling ``get_history()``.

        Note that this event does NOT re-trigger even if the candlestick/data is renewed.
        """
        raise NotImplementedError()

    @abstractmethod
    def on_error(self, message: str) -> None:
        raise NotImplementedError()

    def _quote_subscription_handle_message(self, message: CommonData):
        try:
            match message.data_type:
                case "REALTIME":
                    data = RealtimeData(message)

                    if not data.is_valid:
                        print_warning("[Client] Received invalid (no trade) realtime data")
                        return

                    data = RealtimeData(message)

                    if not self.is_subscribing_realtime(data.symbol_complete):
                        # Subscription is not actively terminated even if the app is exited
                        # Therefore, it is possible to receive realtime data even if it's not subscribed
                        # ------------------------------------------
                        # If such thing happens, ignore that
                        # > Not unsubscribing the data because multiple app instances may run at the same time
                        # > Sending subscription cancellation request will interrupt the other app
                        return

                    self.on_received_realtime_data(data)
                case "TICKS" | "1K" | "DK":
                    handshake = HistoryDataHandshake(message)

                    if not handshake.is_ready:
                        print_warning(f"[Client] Status of history data handshake is not ready ({handshake.status})")
                        return

                    query_idx = 0
                    history_data_of_event = []

                    while True:
                        history_data_list = self.get_paged_history(
                            handshake.symbol_complete, handshake.data_type,
                            handshake.start_time_str, handshake.end_time_str, query_idx
                        ).data

                        if not history_data_list:
                            break

                        history_data_of_event.extend(history_data_list)
                        query_idx = history_data_list[-1].query_idx

                    self.complete_get_history(
                            handshake.symbol_complete, handshake.data_type,
                            handshake.start_time_str, handshake.end_time_str
                    )

                    if history_data_of_event:
                        self.on_received_history_data(HistoryData(
                            data_list=history_data_of_event, handshake=handshake
                        ))
                    else:
                        print_warning(
                            f"[Client] No history data available for "
                            f"[bold]{handshake.symbol_complete}[/bold] ({handshake.data_type})"
                        )
                case "PING" | "UNSUBQUOTE":
                    pass
                case _:
                    print_warning(f"[TC API] Unknown message data type: {message.data_type}")
        except Exception as e:
            print_error(f"[TC API] Error occurred on message received: {message.body}")
            raise e

    def _quote_subscription_loop(self, sub_port: int):
        socket_sub = create_subscription_receiver_socket(sub_port, DATA_TIMEOUT_SEC * 1000)

        while True:
            # Only care about the message after the first color (:)
            message = CommonData(socket_sub.get_message().split(":", 1)[1])

            self._quote_subscription_handle_message(message)
