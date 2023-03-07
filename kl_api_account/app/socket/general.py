import asyncio

from fastapi import HTTPException

from kl_api_common.utils import print_socket_event
from kl_api_account.const import fast_api_socket
from kl_api_account.db import record_session_disconnected
from kl_api_account.endpoints import get_active_user_by_oauth2_token, get_user_config_by_token
from kl_api_account.enums import GeneralSocketEvent
from kl_api_account.model import PxCheckAuthMessage
from kl_api_account.socket import socket_send_to_session
from kl_api_account.utils import to_socket_message_init_data
from .utils import get_tasks_with_session_control, on_http_exception


def register_handlers_general():
    @fast_api_socket.on(GeneralSocketEvent.INIT)
    async def on_request_init_data(session_id: str, access_token: str):
        print_socket_event(GeneralSocketEvent.INIT, session_id=session_id)

        try:
            config = get_user_config_by_token(access_token)

            await asyncio.gather(
                get_tasks_with_session_control(config.account_id, session_id),
                socket_send_to_session(
                    GeneralSocketEvent.INIT,
                    to_socket_message_init_data(config),
                    session_id
                )
            )
        except HTTPException as ex:
            await on_http_exception(ex, session_id)

    @fast_api_socket.on(GeneralSocketEvent.PING)
    async def on_request_ping(session_id: str, *_):
        print_socket_event(GeneralSocketEvent.PING, session_id=session_id)

        await socket_send_to_session(GeneralSocketEvent.PING, "pong", session_id)

    @fast_api_socket.on(GeneralSocketEvent.AUTH)
    async def on_px_check_auth(session_id: str, auth_message: PxCheckAuthMessage):
        try:
            # Calling the method checks token validity
            get_active_user_by_oauth2_token(auth_message["token"])

            await socket_send_to_session(GeneralSocketEvent.AUTH, "OK", session_id)
        except HTTPException as ex:
            await on_http_exception(ex, session_id)
        finally:
            print_socket_event(GeneralSocketEvent.AUTH, session_id=session_id)

    @fast_api_socket.on(GeneralSocketEvent.DISCONNECT)
    async def on_disconnect(session_id: str):
        record_session_disconnected(session_id)
