import asyncio

from kl_site_common.utils import print_error, print_log
from kl_site_server.const import fast_api_socket
from kl_site_server.enums import GeneralSocketEvent, PxSocketEvent
from kl_site_server.model import OnErrorEvent, OnMarketDataReceivedEvent
from kl_site_server.utils import (
    SocketNamespace, get_px_sub_securities_from_room_name, socket_send_to_all, socket_send_to_room,
    to_socket_message_error,
    to_socket_message_px_data_market,
)


async def on_px_data_updated_market(e: OnMarketDataReceivedEvent):
    namespace: SocketNamespace = "/px"
    rooms = fast_api_socket.manager.rooms.get(namespace)

    if not rooms:
        print_log(f"[Server] Px Updated / MKT ({e} - [red]No active rooms[/red])")
        return

    tasks = []
    for room in rooms:
        room_securities = get_px_sub_securities_from_room_name(room)

        if not room_securities:
            continue

        tasks.append(socket_send_to_room(
            PxSocketEvent.UPDATED,
            to_socket_message_px_data_market([
                e.data[security] for security in room_securities if security in e.data
            ]),
            namespace=namespace,
            room=room,
        ))

    if not tasks:
        print_log(f"[Server] Px Updated / MKT ({e} - [red]No active subs[/red])")
        return

    await asyncio.gather(*tasks)

    # `rooms` may contain rooms that are not handling market px update
    print_log(f"[Server] Px Updated / MKT ({e} - {len(tasks)} subs)")


async def on_error(e: OnErrorEvent):
    print_error(f"[Server] Error ({e.message})")
    await socket_send_to_all(GeneralSocketEvent.ERROR, to_socket_message_error(e))
