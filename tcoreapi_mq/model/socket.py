import zmq


class ZmqSocket:
    def __init__(self, zmq_socket: zmq.Socket):
        self.zmq_socket = zmq_socket

    def connect(self, url: str) -> None:
        """Connect to the zmq socket with the given ``url``. ``url`` should include protocol."""
        self.zmq_socket.connect(url)

    def connect_local(self, port: int) -> None:
        """Connect to the zmq socket locally at ``port``."""
        self.connect(f"tcp://127.0.0.1:{port}")

    def set_subscribe(self) -> None:
        self.set_socket_option(zmq.SUBSCRIBE, "")

    def set_socket_option(self, option: zmq.SUBSCRIBE | zmq.UNSUBSCRIBE | zmq.IDENTITY, value: str) -> None:
        """
        Set socket option.

        Value of the socket option ``zmq.SUBSCRIBE`` should be an empty string.
        """
        self.zmq_socket.set_string(option, value)

    def send_string(self, message: str) -> None:
        """Sends a ``message``."""
        self.zmq_socket.send_string(message)

    def get_message(self) -> str:
        """A locking method that gets the socket message."""
        return self.zmq_socket.recv()[:-1].decode("utf-8")
