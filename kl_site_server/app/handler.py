import asyncio
import time
from typing import TYPE_CHECKING

from kl_site_common.utils import print_error, print_log
from kl_site_server.const import fast_api_socket
from kl_site_server.enums import GeneralSocketEvent, PxSocketEvent
from kl_site_server.model import OnErrorEvent, OnMarketDataReceivedEvent, PxData, PxDataConfig
from kl_site_server.socket import SocketNamespace, socket_send_to_all, socket_send_to_room
from kl_site_server.utils import (
    get_px_data_identifiers_from_room_name, get_px_sub_securities_from_room_name, to_socket_message_error,
    to_socket_message_px_data_list, to_socket_message_px_data_market, to_socket_min_change,
)
from tcoreapi_mq.message import SystemTimeData

if TYPE_CHECKING:
    from kl_site_server.client import TouchanceDataClient


async def on_px_data_updated_market(e: OnMarketDataReceivedEvent):
    namespace: SocketNamespace = "/px"
    # Cache `rooms` so the later iterations on it won't crash
    rooms = fast_api_socket.manager.rooms.get(namespace)

    if not rooms:
        print_log(f"Px MKT Updated ({e} - [red]No active rooms[/])")
        return

    tasks = []
    # Cache `rooms` so the later iterations on it won't crash
    for room in list(rooms):
        room_securities = get_px_sub_securities_from_room_name(room)

        if not room_securities:
            continue

        if not (message := to_socket_message_px_data_market(e, room_securities)):
            continue

        tasks.append(socket_send_to_room(PxSocketEvent.UPDATED, message, namespace=namespace, room=room))

    if not tasks:
        print_log(f"Px MKT Updated ({e} - [red]No active subs[/])")
        return

    await asyncio.gather(*tasks)

    # `rooms` may contain rooms that are not handling market px update
    print_log(f"Px MKT Updated ({e} - {len(tasks)} subs)")


async def on_px_data_new_bar_created(
    client: "TouchanceDataClient",
    securities_created: set[str]
):
    if not securities_created:
        print_log("Px BAR Created ([red]No new bars created[/])")
        return

    _start = time.time()
    namespace: SocketNamespace = "/px"
    # Cache `rooms` so the later iterations on it won't crash
    rooms = fast_api_socket.manager.rooms.get(namespace)

    if not rooms:
        print_log("Px BAR Created ([red]No active rooms[/])")
        return

    identifiers = set(identifier for room in rooms for identifier in get_px_data_identifiers_from_room_name(room))

    if not identifiers:
        print_log("Px BAR Created ([red]No active subs[/])")
        return

    px_data_dict: dict[str, PxData] = {
        px_data.unique_identifier: px_data
        for px_data in client.get_px_data(PxDataConfig.from_unique_identifiers(
            identifiers, securities_to_include=securities_created
        ))
    }

    tasks = []
    # Cache `rooms` so the later iterations on it won't crash
    for room in list(rooms):
        room_identifiers = get_px_data_identifiers_from_room_name(room)

        if not room_identifiers:
            continue

        px_data_list = [
            px_data_dict[identifier] for identifier in room_identifiers
            if identifier in px_data_dict  # Identifiers that do not have new bars created are skipped
        ]

        if not px_data_list:
            continue

        tasks.append(socket_send_to_room(
            PxSocketEvent.REQUEST,
            to_socket_message_px_data_list(px_data_list),
            namespace=namespace,
            room=room,
        ))

    if not tasks:
        print_log("Px BAR Created ([red]No active rooms[/])")
        return

    await asyncio.gather(*tasks)

    print_log(
        f"Px BAR Created - {time.time() - _start:.3f} s "
        f"({len(px_data_dict)} / [blue]{', '.join(sorted(identifiers))}[/])"
    )


async def on_system_time_min_change(data: SystemTimeData):
    namespace: SocketNamespace = "/px"

    print_log(f"Server minute change to {data.timestamp} ({data.epoch_sec})")
    await socket_send_to_all(PxSocketEvent.MIN_CHANGE, to_socket_min_change(data), namespace=namespace)


async def on_error(e: OnErrorEvent):
    print_error(f"Error ({e.message})")
    await socket_send_to_all(GeneralSocketEvent.ERROR, to_socket_message_error(e))
