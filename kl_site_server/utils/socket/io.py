from kl_site_server.const import fast_api_socket
from .channels import SocketNamespace


async def socket_send_to_session(event: str, data: str | bytes, session_id: str):
    await fast_api_socket.emit(event, data, to=session_id)


async def socket_send_to_room(event: str, data: str | bytes, *, namespace: SocketNamespace = "/", room: str):
    await fast_api_socket.emit(event, data, namespace=namespace, room=room)


async def socket_send_to_all(event: str, data: str | bytes):
    await fast_api_socket.emit(event, data)
