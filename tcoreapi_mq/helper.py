import threading
from typing import TYPE_CHECKING

from .utils import create_subscription_receiver_socket

if TYPE_CHECKING:
    from .core import TCoreZMQ


class KeepAliveHelper:
    def __init__(self, sub_port: int, tcore_zmq: "TCoreZMQ"):
        threading.Thread(target=self._thread_process, args=(sub_port, tcore_zmq)).start()

        self.is_terminated = False

    def close(self) -> None:
        self.is_terminated = True

    def _thread_process(self, sub_port: int, tcore_zmq: "TCoreZMQ") -> None:
        socket = create_subscription_receiver_socket(sub_port)

        while True:
            message = socket.get_message()

            # Expect message to be {"DataType": "PING"}
            if "PING" not in message:
                continue

            if self.is_terminated:
                return

            tcore_zmq.pong("TC")
