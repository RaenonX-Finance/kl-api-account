from kl_site_common.utils import print_log
from kl_site_server.enums import MarketPxSocketEvent, PxSocketEvent
from kl_site_server.model import OnErrorEvent, OnMarketDataReceivedEvent
from kl_site_server.utils import (
    socket_send_to_all, socket_send_to_room, to_socket_message_error, to_socket_message_px_data_market,
)


async def on_px_data_updated_market(e: OnMarketDataReceivedEvent):
    print_log(f"[Server] Px Updated / MKT ({e})")
    await socket_send_to_room(
        MarketPxSocketEvent.UPDATED,
        to_socket_message_px_data_market(e.data),
        namespace="/px-market",
        room=e.security,
    )


async def on_error(e: OnErrorEvent):
    await socket_send_to_all(PxSocketEvent.ERROR, to_socket_message_error(e))
