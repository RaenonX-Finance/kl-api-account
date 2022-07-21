from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_user_config_by_token
from kl_site_server.enums import GeneralSocketEvent
from kl_site_server.utils import SocketNamespace, socket_send_to_session, to_socket_message_init_data


def register_handlers_general():
    namespace: SocketNamespace = "/"

    @fast_api_socket.on(GeneralSocketEvent.INIT, namespace=namespace)
    async def on_request_init_data(session_id: str, access_token: str):
        print_socket_event(GeneralSocketEvent.INIT, session_id=session_id, namespace=namespace)

        try:
            config = get_user_config_by_token(access_token)
            await socket_send_to_session(
                GeneralSocketEvent.INIT,
                to_socket_message_init_data(config),
                session_id
            )
        except HTTPException as ex:
            await socket_send_to_session(GeneralSocketEvent.SIGN_IN, ex.detail, session_id)

    @fast_api_socket.on(GeneralSocketEvent.PING, namespace=namespace)
    async def on_request_ping(session_id: str, *_):
        print_socket_event(GeneralSocketEvent.PING, session_id=session_id, namespace=namespace)

        await socket_send_to_session(GeneralSocketEvent.PING, "pong", session_id)
