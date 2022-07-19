from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.client import TouchanceDataClient
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_user_config_by_token
from kl_site_server.enums import PxSocketEvent
from kl_site_server.model import PxDataConfig
from kl_site_server.utils import (
    SocketNamespace, socket_send_to_session, to_socket_message_init_data,
    to_socket_message_px_data_list,
)


def register_handlers_px(client: TouchanceDataClient):
    namespace: SocketNamespace = "/"

    @fast_api_socket.on(PxSocketEvent.INIT, namespace=namespace)
    async def on_request_init_data(session_id: str, access_token: str):
        print_socket_event(PxSocketEvent.INIT, session_id=session_id, namespace=namespace)

        try:
            config = get_user_config_by_token(access_token)
            await socket_send_to_session(
                PxSocketEvent.INIT,
                to_socket_message_init_data(config),
                session_id
            )
        except HTTPException as ex:
            await socket_send_to_session(PxSocketEvent.SIGN_IN, ex.detail, session_id)

    @fast_api_socket.on(PxSocketEvent.PX_INIT, namespace=namespace)
    async def on_request_px_data_init(session_id: str, access_token: str):
        print_socket_event(PxSocketEvent.PX_INIT, session_id=session_id, namespace=namespace)

        try:
            config = get_user_config_by_token(access_token)
            await socket_send_to_session(
                PxSocketEvent.PX_INIT,
                to_socket_message_px_data_list(client.get_px_data(PxDataConfig.from_config(config))),
                session_id
            )
        except HTTPException as ex:
            await socket_send_to_session(PxSocketEvent.SIGN_IN, ex.detail, session_id)

    @fast_api_socket.on(PxSocketEvent.PING, namespace=namespace)
    async def on_request_ping(session_id: str, *_):
        print_socket_event(PxSocketEvent.PING, session_id=session_id, namespace=namespace)

        await socket_send_to_session(PxSocketEvent.PING, "pong", session_id)
