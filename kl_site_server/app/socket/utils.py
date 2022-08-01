import asyncio
from typing import Any, Coroutine

from fastapi import HTTPException

from kl_site_common.db import PyObjectId
from kl_site_server.db import record_session_connected
from kl_site_server.enums import GeneralSocketEvent
from kl_site_server.utils import SocketNamespace, socket_disconnect_session, socket_send_to_session


async def on_http_exception(ex: HTTPException, session_id: str, namespace: SocketNamespace):
    await asyncio.gather(
        socket_send_to_session(GeneralSocketEvent.SIGN_IN, ex.detail, session_id),
        socket_disconnect_session(session_id, namespace=namespace)
    )


def get_tasks_with_session_control(
    account_id: PyObjectId,
    namespace: SocketNamespace,
    session_id: str
) -> list[Coroutine[Any, Any, None]]:
    session_ids_to_disconnect = record_session_connected(account_id, namespace, session_id)

    return [
        socket_disconnect_session(session_id, namespace=ns)
        for ns, session_id in (session_ids_to_disconnect or {}).items()
    ]
