from kl_site_server.const import fast_api_socket
from .channels import SocketNamespace


async def socket_send_to_session(
    event: str,
    data: str | bytes,
    session_id: str, *,
    namespace: SocketNamespace = "/"
):
    await fast_api_socket.emit(event, data, to=session_id, namespace=namespace)


async def socket_send_to_room(
    event: str,
    data: str | bytes, *,
    namespace: SocketNamespace = "/",
    room: str | list[str]
):
    await fast_api_socket.emit(event, data, namespace=namespace, room=room)


async def socket_send_to_all(event: str, data: str | bytes, *, namespace: SocketNamespace = "/"):
    await fast_api_socket.emit(event, data, namespace=namespace)


def socket_join_room(session_id: str, room: str, namespace: SocketNamespace):
    fast_api_socket.enter_room(session_id, room, namespace)


def socket_leave_room(session_id: str, room: str, namespace: SocketNamespace):
    fast_api_socket.leave_room(session_id, room, namespace)
