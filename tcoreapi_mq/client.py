from abc import ABC, abstractmethod
from datetime import datetime
from threading import Thread

from kl_site_common.const import DATA_TIMEOUT_SEC, SYS_PORT_QUOTE
from kl_site_common.utils import print_error, print_warning
from .message import CommonData, HistoryData, HistoryDataHandshake, PxHistoryDataEntry, RealtimeData, SystemTimeData
from .quote_api import QuoteAPI
from .utils import create_subscription_receiver_socket


class TouchanceApiClient(QuoteAPI, ABC):
    def start(self):
        login_result = self.connect(SYS_PORT_QUOTE)

        if not login_result.success:
            raise RuntimeError("Px quoting connection failed")

        Thread(target=self._quote_subscription_loop, args=(login_result.sub_port,)).start()

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
    def on_system_time_min_change(self, data: SystemTimeData) -> None:
        """This method is triggered exactly every minute."""
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
                        print_warning(f"Received invalid (no trade) realtime data from {data.security}")
                        return

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
                        print_warning(f"Status of history data handshake is not ready ({handshake.status})")
                        return

                    query_idx = 0
                    # Use `dict` to ensure no duplicates
                    # > Paged history may contain duplicated data, say last of page 0 and first of page 1
                    history_data_of_event: dict[datetime, PxHistoryDataEntry] = {}

                    while True:
                        history_data_paged = self.get_paged_history(handshake, query_idx)

                        if not history_data_paged.data:
                            break

                        history_data_of_event.update(history_data_paged.data)
                        query_idx = history_data_paged.last_query_idx

                    self.complete_get_history(handshake)

                    if history_data_of_event:
                        self.on_received_history_data(HistoryData.from_socket_message(
                            list(history_data_of_event.values()),
                            handshake
                        ))
                    else:
                        print_error(
                            f"No history data available for "
                            f"[bold]{handshake.symbol_complete}[/] ({handshake.data_type})"
                        )
                case "PING" | "UNSUBQUOTE":
                    pass
                case "SYSTEMTIME":
                    self.on_system_time_min_change(SystemTimeData(message))
                case _:
                    print_warning(f"Unknown message data type: {message.data_type}")
        except Exception as e:
            print_error(f"Error occurred on message received: {message.body}")
            self.on_error(f"Error occurred on receiving message type: {message.data_type} ({e.args})")
            raise e

    def _quote_subscription_loop(self, sub_port: int):
        socket_sub = create_subscription_receiver_socket(sub_port, DATA_TIMEOUT_SEC * 1000)

        while True:
            # Only care about the message after the first color (:)
            message = CommonData(socket_sub.get_message().split(":", 1)[1])

            self._quote_subscription_handle_message(message)
