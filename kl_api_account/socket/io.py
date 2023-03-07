from kl_api_account.const import fast_api_socket


async def socket_disconnect_session(session_id: str):
    await fast_api_socket.disconnect(session_id)


async def socket_send_to_session(event: str, data: str | bytes | list | dict, session_id: str):
    await fast_api_socket.emit(event, data, to=session_id)


async def socket_send_to_room(event: str, data: str | bytes, *, room: str | list[str]):
    await fast_api_socket.emit(event, data, room=room)


async def socket_send_to_all(event: str, data: str | bytes):
    await fast_api_socket.emit(event, data)
