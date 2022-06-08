from kl_site_common.utils import print_log
from kl_site_server.const import fast_api_socket
from kl_site_server.enums import SocketEvent
from kl_site_server.model import OnErrorEvent, OnMarketDataReceivedEvent, OnPxDataUpdatedEvent
from kl_site_server.utils import to_socket_message_error, to_socket_message_px_data, to_socket_message_px_data_market


async def on_px_data_updated(e: OnPxDataUpdatedEvent):
    print_log(f"[TWS] Px Updated / HST ({e})")
    await fast_api_socket.emit(
        SocketEvent.PX_UPDATED,
        to_socket_message_px_data(e.px_data)
    )


async def on_px_data_updated_market(e: OnMarketDataReceivedEvent):
    print_log(f"[TWS] Px Updated / MKT ({e})")
    await fast_api_socket.emit(
        SocketEvent.PX_UPDATED_MARKET,
        to_socket_message_px_data_market(e.symbol, e.px)
    )


async def on_error(e: OnErrorEvent):
    await fast_api_socket.emit(SocketEvent.ERROR, to_socket_message_error(e))
