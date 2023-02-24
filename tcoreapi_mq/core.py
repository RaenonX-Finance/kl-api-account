from threading import Lock
from typing import Optional

import zmq

from kl_site_common.utils import print_log

from .helper import KeepAliveHelper
from .message import (
    ErrorMessage, InstrumentType, LoginMessage, LoginRequest, LogoutRequest, PongMessage, PongRequst,
    QueryAllInstrumentRequest, QueryInstrumentMessage, QueryInstrumentRequest,
)
from .model import SymbolBaseType, ZmqSocket
from .utils import create_socket


class TCoreZMQ:
    def __init__(self, app_id: str, service_key: str):
        self.socket: ZmqSocket = create_socket(zmq.REQ)
        self.app_id: str = app_id
        self.service_key: str = service_key
        self.session_key_internal: str | None = None

        self.lock: Lock = Lock()
        self.obj_zmq_keep_alive: Optional[KeepAliveHelper] = None

    @property
    def session_key(self) -> str:
        if not self.session_key_internal:
            raise ValueError("Session key is `None` - API not connected")

        return self.session_key_internal

    def connect(self, port: int) -> LoginMessage:
        """Connect to Touchance with specific `port`."""
        with self.lock:
            self.socket.connect_local(port)
            self.socket.send_string(LoginRequest(app_id=self.app_id, service_key=self.service_key).to_message())

            data = LoginMessage(message=self.socket.get_message())

        self.session_key_internal = data.session_key

        if data.success:
            self.create_ping_pong(data.sub_port)

        print_log(f"Connected to port {data.sub_port}.")
        print_log(f"Session Key: [yellow]{data.session_key}[/]")

        return data

    def create_ping_pong(self, sub_port: int) -> None:
        """Create ping pong message."""
        if self.obj_zmq_keep_alive is not None:
            self.obj_zmq_keep_alive.close()

        print_log(f"Created ping pong helper at port {sub_port}.")
        self.obj_zmq_keep_alive = KeepAliveHelper(sub_port, self)

    def pong(self, id_: str) -> PongMessage:
        """Called when received "ping"."""
        with self.lock:
            self.socket.send_string(PongRequst(session_key=self.session_key, id_=id_).to_message())

            return PongMessage(message=self.socket.get_message())

    def logout(self) -> None:
        with self.lock:
            self.socket.send_string(LogoutRequest(session_key=self.session_key).to_message())

        print_log("Disconnected.")
        self.session_key_internal = None

    def query_instrument_info(self, symbol_obj: SymbolBaseType) -> QueryInstrumentMessage:
        print_log(f"Requesting instrument info of [yellow]{symbol_obj.security}[/]")

        with self.lock:
            self.socket.send_string(QueryInstrumentRequest(
                session_key=self.session_key, symbol_obj=symbol_obj
            ).to_message())
            return QueryInstrumentMessage(symbol_obj=symbol_obj, message=self.socket.get_message())
