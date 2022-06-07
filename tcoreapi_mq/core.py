from typing import Optional

import zmq
import threading

from .helper import KeepAliveHelper
from .message import (
    LoginRequest, LoginMessage,
    LogoutRequest,
    QueryInstrumentRequest, QueryInstrumentMessage,
    QueryAllInstrumentRequest,
    ErrorMessage,
    PongRequst, PongMessage,
)
from .model import SymbolBaseType
from tcoreapi_mq.message.send.types import InstrumentType


class TCoreZMQ:
    def __init__(self, app_id: str, service_key: str):
        self.context: zmq.Context = zmq.Context()
        self.socket: zmq.Socket = self.context.socket(zmq.REQ)
        self.app_id: str = app_id
        self.service_key: str = service_key

        self.lock: threading.Lock = threading.Lock()
        self.obj_zmq_keep_alive: Optional[KeepAliveHelper] = None

    def connect(self, port: int) -> LoginMessage:
        """Connect to Touchance with specific `port`."""
        with self.lock:
            self.socket.connect(f"tcp://127.0.0.1:{port}")
            self.socket.send_string(LoginRequest(app_id=self.app_id, service_key=self.service_key).to_message())

            data = LoginMessage(message=self.socket.recv()[:-1])

        if data.success:
            self.create_ping_pong(data.session_key, data.sub_port)

        return data

    def create_ping_pong(self, session_key: str, sub_port: int) -> None:
        """Create ping pong message."""
        if self.obj_zmq_keep_alive is not None:
            self.obj_zmq_keep_alive.close()

        self.obj_zmq_keep_alive = KeepAliveHelper(sub_port, session_key, self)

    def logout(self, session_key: str) -> None:
        with self.lock:
            self.socket.send_string(LogoutRequest(session_key=session_key).to_message())

    def query_instrument_info(self, session_key: str, symbol: SymbolBaseType) -> QueryInstrumentMessage:
        with self.lock:
            self.socket.send_string(QueryInstrumentRequest(session_key=session_key, symbol=symbol).to_message())
            return QueryInstrumentMessage(message=self.socket.recv()[:-1])

    def query_all_instrument_info(self, session_key: str, instrument_type: InstrumentType) -> ErrorMessage:
        with self.lock:
            req = QueryAllInstrumentRequest(session_key=session_key, instrument_type=instrument_type)
            self.socket.send_string(req.to_message())

            # FIXME: Temporarily returns error message only, none of the instrument type works
            return ErrorMessage(message=self.socket.recv()[:-1])

    def pong(self, session_key: str, id_: str) -> PongMessage:
        """Called when received "ping"."""
        with self.lock:
            self.socket.send_string(PongRequst(session_key=session_key, id_=id_).to_message())

            return PongMessage(message=self.socket.recv()[:-1])
