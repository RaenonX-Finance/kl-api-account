from abc import ABC, abstractmethod
from datetime import datetime
from threading import Thread

from kl_site_common.const import DATA_TIMEOUT_SEC, SYS_PORT_QUOTE
from kl_site_common.utils import print_debug, print_error, print_log, print_warning
from .message import CommonData, HistoryData, HistoryDataHandshake, PxHistoryDataEntry, RealtimeData, SystemTimeData
from .quote_api import QuoteAPI
from .utils import create_subscription_receiver_socket


class TouchanceApiClient(QuoteAPI, ABC):
    def __init__(self):
        super().__init__()

        self._last_data_min: int = datetime.utcnow().minute

    def start(self):
        login_result = self.connect(SYS_PORT_QUOTE)

        if not login_result.success:
            raise RuntimeError("Px quoting connection failed")

        Thread(target=self._quote_subscription_loop, args=(login_result.sub_port,)).start()

    @abstractmethod
    def on_received_realtime_data(self, data: RealtimeData) -> None:
        raise NotImplementedError()

    @abstractmethod
    def on_received_history_data(self, data: HistoryData, handshake: HistoryDataHandshake) -> None:
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

    def test_minute_change(self, timestamp: datetime):
        prev_min = self._last_data_min
        self._last_data_min = timestamp.minute

        if prev_min != self._last_data_min:
            print_log(f"Server minute change - changing from {prev_min} to {self._last_data_min} on {timestamp}")
            self.on_system_time_min_change(SystemTimeData.from_datetime(timestamp))

    def _quote_subscription_handle_message(self, message: CommonData):
        try:
            match message.data_type:
                case "REALTIME":
                    data = RealtimeData(message)

                    if not data.is_valid:
                        print_warning(f"Received invalid (no trade) realtime data from {data.security}")
                        return

                    print_log(
                        f"Last: {data.last_px} / Close: {data.close} / Change: {data.change_val}",
                        identifier=f"RTM-[yellow]{data.symbol_complete}[/]"
                    )

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
                        print_warning(
                            f"Status of history data handshake is not ready ({handshake.status})",
                            identifier=handshake.request_identifier,
                        )
                        return

                    if not self.is_handshake_valid_request(handshake):
                        print_debug(
                            "Received not subscribed history data handshake",
                            identifier=handshake.request_identifier,
                        )
                        return

                    query_idx = 0
                    # Use `dict` to ensure no duplicates
                    # > Paged history may contain duplicated data, say last of page 0 and first of page 1
                    history_data_of_event: dict[datetime, PxHistoryDataEntry] = {}

                    while True:
                        history_data_paged = self.get_paged_history(handshake, query_idx)
                        if query_idx % 3000 == 0:
                            print_log(
                                f"Received history data at index #{query_idx} ({len(history_data_paged.data)})",
                                identifier=handshake.request_identifier,
                            )

                        if not history_data_paged:
                            return

                        if not history_data_paged.data:
                            break

                        history_data_of_event.update(history_data_paged.data)
                        query_idx = history_data_paged.last_query_idx

                    print_log(
                        f"Received {len(history_data_of_event)} history data",
                        identifier=handshake.request_identifier,
                    )

                    self.complete_get_history(handshake)

                    if history_data_of_event:
                        self.on_received_history_data(
                            HistoryData.from_socket_message(list(history_data_of_event.values()), handshake),
                            handshake
                        )
                    else:
                        print_error("No history data available", identifier=handshake.request_identifier)
                case "PING" | "UNSUBQUOTE" | "SYSTEMTIME":
                    pass
                case _:
                    print_warning(f"Unknown message data type: {message.data_type}")
        except Exception as e:
            print_error(f"Error occurred on message received: {message.body}")
            self.on_error(f"Error occurred on receiving message type: {message.data_type} ({e.args})")
            raise e

    def _quote_subscription_loop(self, sub_port: int):
        socket_sub = create_subscription_receiver_socket(sub_port, DATA_TIMEOUT_SEC * 1000)

        while True:
            # Only care about the message after the first colon (:)
            message = CommonData(socket_sub.get_message().split(":", 1)[1])

            self._quote_subscription_handle_message(message)
