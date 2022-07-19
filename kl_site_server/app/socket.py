from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.client import TouchanceDataClient
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_user_config_by_token
from kl_site_server.enums import SocketEvent
from kl_site_server.model import PxDataConfig
from kl_site_server.utils import socket_send_to_session, to_socket_message_init_data, to_socket_message_px_data_list


def register_handlers(client: TouchanceDataClient):
    @fast_api_socket.on(SocketEvent.INIT)
    async def on_request_init_data(session_id: str, access_token: str):
        print_socket_event(SocketEvent.INIT, session_id=session_id)

        try:
            config = get_user_config_by_token(access_token)
            await socket_send_to_session(
                SocketEvent.INIT,
                to_socket_message_init_data(config),
                session_id
            )
        except HTTPException as ex:
            await fast_api_socket.emit(SocketEvent.SIGN_IN, ex.detail, to=session_id)

    @fast_api_socket.on(SocketEvent.PX_INIT)
    async def on_request_px_data_init(session_id: str, access_token: str):
        print_socket_event(SocketEvent.PX_INIT, session_id=session_id)

        try:
            config = get_user_config_by_token(access_token)
            await socket_send_to_session(
                SocketEvent.PX_INIT,
                to_socket_message_px_data_list(client.get_px_data(PxDataConfig.from_config(config))),
                session_id
            )
        except HTTPException as ex:
            await fast_api_socket.emit(SocketEvent.SIGN_IN, ex.detail, to=session_id)

    @fast_api_socket.on(SocketEvent.PING)
    async def on_request_ping(session_id: str, *_):
        print_socket_event(SocketEvent.PING, session_id=session_id)

        await socket_send_to_session(SocketEvent.PING, "pong", session_id)
