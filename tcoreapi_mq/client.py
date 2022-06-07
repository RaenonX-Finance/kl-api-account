import threading
from abc import ABC

from .const import DATA_TIMEOUT_SECS, SYS_APP_ID, SYS_PORT_QUOTE, SYS_SERVICE_KEY
from .message import CommonData, HistoryData, HistoryDataHandshake, RealtimeData
from .quote_api import QuoteAPI
from .utils import create_subscription_receiver_socket, print_log, print_warning


class TocuhanceApiClient(QuoteAPI, ABC):
    def __init__(self):
        super().__init__(SYS_APP_ID, SYS_SERVICE_KEY)

    def start(self):
        login_result = self.connect(SYS_PORT_QUOTE)

        if not login_result.success:
            raise RuntimeError("Px quoting connection failed")

        t2 = threading.Thread(target=self._quote_subscription_loop, args=(login_result.sub_port,))
        t2.start()

    def on_received_realtime_data(self, data: RealtimeData):
        pass

    def on_received_history_data(self, data: HistoryData):
        pass

    def _quote_subscription_handle_message(self, message: CommonData):
        print_log(f"[Client] Received Px quote of type: [purple]{message.data_type}[/purple]")

        match message.data_type:
            case "REALTIME":
                self.on_received_realtime_data(RealtimeData(message))
            case "TICKS" | "1K" | "DK":
                handshake = HistoryDataHandshake(message)

                if not handshake.is_ready:
                    print_warning(f"[Client] Status of history data handshake is not ready ({handshake.status})")
                    return

                query_idx = 0
                history_data_of_event = []

                while True:
                    history_data_list = self.get_paged_history(
                        handshake.symbol_name, handshake.data_type,
                        handshake.start_time_str, handshake.end_time_str, query_idx
                    ).data

                    if not history_data_list:
                        break

                    history_data_of_event.extend(history_data_list)
                    query_idx = history_data_list[-1].query_idx

                self.on_received_history_data(HistoryData(history_data_of_event))
            case "PING":
                pass
            case _:
                print_warning(f"[Client] Unknown message data type: {message.data_type}")

    def _quote_subscription_loop(self, sub_port: int):
        socket_sub = create_subscription_receiver_socket(sub_port, DATA_TIMEOUT_SECS * 1000)

        while True:
            # Only care about the message after the first color (:)
            message = CommonData(socket_sub.get_message().split(":", 1)[1])

            self._quote_subscription_handle_message(message)
