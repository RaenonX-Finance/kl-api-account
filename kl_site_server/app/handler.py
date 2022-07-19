from kl_site_common.utils import print_error, print_log
from kl_site_server.enums import GeneralSocketEvent, PxSocketEvent
from kl_site_server.model import OnErrorEvent, OnMarketDataReceivedEvent
from kl_site_server.utils import (
    socket_send_to_all, socket_send_to_room, to_socket_message_error, to_socket_message_px_data_market,
)


async def on_px_data_updated_market(e: OnMarketDataReceivedEvent):
    print_log(f"[Server] Px Updated / MKT ({e})")
    await socket_send_to_room(
        PxSocketEvent.UPDATED,
        to_socket_message_px_data_market(e.data),
        namespace="/px",
        room=e.security,
    )


async def on_error(e: OnErrorEvent):
    print_error(f"[Server] Error ({e.message})")
    await socket_send_to_all(GeneralSocketEvent.ERROR, to_socket_message_error(e))
