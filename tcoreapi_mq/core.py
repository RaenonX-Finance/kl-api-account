import threading
from typing import Optional

import zmq

from .helper import KeepAliveHelper
from .message import (
    ErrorMessage, InstrumentType, LoginMessage, LoginRequest, LogoutRequest, PongMessage, PongRequst,
    QueryAllInstrumentRequest, QueryInstrumentMessage, QueryInstrumentRequest,
)
from .model import SymbolBaseType, ZmqSocket
from .utils import print_log, create_socket


class TCoreZMQ:
    def __init__(self, app_id: str, service_key: str):
        self.socket: ZmqSocket = create_socket(zmq.REQ)
        self.app_id: str = app_id
        self.service_key: str = service_key
        self.session_key_internal: str | None = None

        self.lock: threading.Lock = threading.Lock()
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

        print_log(f"[API] Connected to port {data.sub_port}.")
        print_log(f"[API] Session Key: [yellow]{data.session_key}[/yellow]")

        return data

    def create_ping_pong(self, sub_port: int) -> None:
        """Create ping pong message."""
        if self.obj_zmq_keep_alive is not None:
            self.obj_zmq_keep_alive.close()

        print_log(f"[API] created ping pong helper at port {sub_port}.")
        self.obj_zmq_keep_alive = KeepAliveHelper(sub_port, self)

    def pong(self, id_: str) -> PongMessage:
        """Called when received "ping"."""
        with self.lock:
            self.socket.send_string(PongRequst(session_key=self.session_key, id_=id_).to_message())

            return PongMessage(message=self.socket.get_message())

    def logout(self) -> None:
        with self.lock:
            self.socket.send_string(LogoutRequest(session_key=self.session_key).to_message())

        print_log("[API] Disconnected.")
        self.session_key_internal = None

    def query_instrument_info(self, symbol: SymbolBaseType) -> QueryInstrumentMessage:
        print_log(f"[API] Requesting instrument info of [yellow]{symbol.symbol_name}[/yellow]")

        with self.lock:
            self.socket.send_string(QueryInstrumentRequest(session_key=self.session_key, symbol=symbol).to_message())
            return QueryInstrumentMessage(message=self.socket.get_message())

    def query_all_instrument_info(self, instrument_type: InstrumentType) -> ErrorMessage:
        print_log(f"[API] Requesting all instrument info of type [yellow]{instrument_type}[/yellow]")

        with self.lock:
            req = QueryAllInstrumentRequest(session_key=self.session_key, instrument_type=instrument_type)
            self.socket.send_string(req.to_message())

            # FIXME: Temporarily returns error message only, none of the instrument type works
            return ErrorMessage(message=self.socket.get_message())
