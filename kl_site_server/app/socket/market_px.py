from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_active_user_by_oauth2_token
from kl_site_server.enums import MarketPxSocketEvent, PxSocketEvent
from kl_site_server.model import MarketPxSubscriptionMessage
from kl_site_server.utils import SocketNamespace, socket_join_room, socket_leave_room, socket_send_to_session


def register_handlers_market_px():
    namespace: SocketNamespace = "/px-market"

    @fast_api_socket.on(MarketPxSocketEvent.SUBSCRIBE, namespace=namespace)
    async def on_market_px_subscribe(session_id: str, message: str):
        subscription_message = MarketPxSubscriptionMessage.from_message(message)

        print_socket_event(
            MarketPxSocketEvent.SUBSCRIBE,
            session_id=session_id, namespace=namespace, additional=subscription_message.security,
        )

        try:
            get_active_user_by_oauth2_token(subscription_message.token)
            socket_join_room(session_id, subscription_message.security, namespace)
        except HTTPException as ex:
            await socket_send_to_session(PxSocketEvent.SIGN_IN, ex.detail, session_id)

    @fast_api_socket.on(MarketPxSocketEvent.UNSUBSCRIBE, namespace=namespace)
    async def on_request_ping(session_id: str, message: str):
        subscription_message = MarketPxSubscriptionMessage.from_message(message)

        print_socket_event(
            MarketPxSocketEvent.UNSUBSCRIBE,
            session_id=session_id, namespace=namespace, additional=subscription_message.security,
        )

        socket_leave_room(session_id, subscription_message.security, namespace)
