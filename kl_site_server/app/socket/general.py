import asyncio

from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.const import fast_api_socket
from kl_site_server.db import record_session_checked, record_session_disconnected
from kl_site_server.endpoints import get_active_user_by_oauth2_token, get_user_config_by_token
from kl_site_server.enums import GeneralSocketEvent
from kl_site_server.model import PxCheckAuthMessage
from kl_site_server.utils import SocketNamespace, socket_send_to_session, to_socket_message_init_data
from .utils import get_tasks_with_session_control, on_http_exception


def register_handlers_general():
    namespace: SocketNamespace = "/"

    @fast_api_socket.on(GeneralSocketEvent.INIT, namespace=namespace)
    async def on_request_init_data(session_id: str, access_token: str):
        print_socket_event(GeneralSocketEvent.INIT, session_id=session_id, namespace=namespace)

        try:
            config = get_user_config_by_token(access_token)

            await asyncio.gather(
                *get_tasks_with_session_control(config.account_id, namespace, session_id),
                socket_send_to_session(
                    GeneralSocketEvent.INIT,
                    to_socket_message_init_data(config),
                    session_id
                )
            )
        except HTTPException as ex:
            await on_http_exception(ex, session_id, namespace)

    @fast_api_socket.on(GeneralSocketEvent.PING, namespace=namespace)
    async def on_request_ping(session_id: str, *_):
        print_socket_event(GeneralSocketEvent.PING, session_id=session_id, namespace=namespace)

        await socket_send_to_session(GeneralSocketEvent.PING, "pong", session_id)

    @fast_api_socket.on(GeneralSocketEvent.AUTH, namespace=namespace)
    async def on_px_check_auth(session_id: str, auth_message: PxCheckAuthMessage):
        try:
            user_data = get_active_user_by_oauth2_token(auth_message["token"])
            record_session_checked(user_data.id)

            await socket_send_to_session(GeneralSocketEvent.AUTH, "OK", session_id, namespace=namespace)
        except HTTPException as ex:
            await on_http_exception(ex, session_id, namespace)
        finally:
            print_socket_event(GeneralSocketEvent.AUTH, session_id=session_id, namespace=namespace)

    @fast_api_socket.on(GeneralSocketEvent.DISCONNECT, namespace=namespace)
    async def on_disconnect(session_id: str):
        record_session_disconnected(namespace, session_id)
