from typing import Any, Coroutine

from fastapi import HTTPException

from kl_api_common.db import PyObjectId
from kl_api_account.db import record_session_connected
from kl_api_account.enums import GeneralSocketEvent
from kl_api_account.socket import socket_disconnect_session, socket_send_to_session


async def on_http_exception(ex: HTTPException, session_id: str):
    # Can't use `asyncio.gather()` here because sign-in event should be sent first
    await socket_send_to_session(GeneralSocketEvent.SIGN_IN, ex.detail, session_id)
    await socket_disconnect_session(session_id)


def get_tasks_with_session_control(account_id: PyObjectId, session_id: str) -> list[Coroutine[Any, Any, None]]:
    session_id_to_disconnect = record_session_connected(account_id, session_id)

    return [socket_disconnect_session(session_id_to_disconnect)]
