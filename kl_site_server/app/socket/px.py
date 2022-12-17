from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.const import fast_api_socket
from kl_site_server.db import record_session_disconnected
from kl_site_server.endpoints import get_active_user_by_oauth2_token
from kl_site_server.enums import PxSocketEvent
from kl_site_server.model import MarketPxSubscriptionMessage
from kl_site_server.socket import SocketNamespace, socket_join_room, socket_leave_room
from kl_site_server.utils import make_px_data_room_name, make_px_sub_room_name
from .utils import on_http_exception


def register_handlers_px():
    namespace: SocketNamespace = "/px"

    @fast_api_socket.on(PxSocketEvent.SUBSCRIBE, namespace=namespace)
    async def on_market_px_subscribe(session_id: str, subscription_message: MarketPxSubscriptionMessage):
        identifiers = subscription_message["identifiers"]
        print_socket_event(
            PxSocketEvent.SUBSCRIBE,
            session_id=session_id, namespace=namespace, additional=f"[yellow]{identifiers}[/]",
        )

        try:
            get_active_user_by_oauth2_token(subscription_message["token"])

            socket_join_room(session_id, make_px_sub_room_name(identifiers), namespace)
            socket_join_room(session_id, make_px_data_room_name(identifiers), namespace)
        except HTTPException as ex:
            await on_http_exception(ex, session_id, namespace)

    @fast_api_socket.on(PxSocketEvent.UNSUBSCRIBE, namespace=namespace)
    async def on_market_px_unsubscribe(session_id: str, subscription_message: MarketPxSubscriptionMessage):
        identifiers = subscription_message["identifiers"]
        print_socket_event(
            PxSocketEvent.UNSUBSCRIBE,
            session_id=session_id, namespace=namespace, additional=f"[yellow]{identifiers}[/]",
        )

        socket_leave_room(session_id, make_px_sub_room_name(identifiers), namespace)
        socket_leave_room(session_id, make_px_data_room_name(identifiers), namespace)

    @fast_api_socket.on(PxSocketEvent.DISCONNECT, namespace=namespace)
    async def on_disconnect(session_id: str):
        record_session_disconnected(namespace, session_id)
