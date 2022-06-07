import re
import threading
from typing import TYPE_CHECKING

import zmq

if TYPE_CHECKING:
    from .core import TCoreZMQ


class KeepAliveHelper:
    def __init__(self, sub_port: int, session_key: str, tcore_zmq: "TCoreZMQ"):
        threading.Thread(target=self.thread_process, args=(sub_port, session_key, tcore_zmq)).start()

        self.is_terminated = False

    def close(self):
        self.is_terminated = True

    def thread_process(self, sub_port: int, session_key: str, tcore_zmq: "TCoreZMQ"):
        socket_sub = zmq.Context().socket(zmq.SUB)
        socket_sub.connect(f"tcp://127.0.0.1:{sub_port}")
        socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        while True:
            message = (socket_sub.recv()[:-1]).decode("utf-8")
            find_text = re.search("{\"DataType\":\"PING\"}", message)

            if find_text is None:
                continue

            if self.is_terminated:
                return

            tcore_zmq.pong(session_key, "TC")
