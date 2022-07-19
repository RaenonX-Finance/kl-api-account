from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.client import TouchanceDataClient
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_active_user_by_oauth2_token
from kl_site_server.enums import MarketPxSocketEvent, PxSocketEvent
from kl_site_server.model import MarketPxSubscriptionMessage, PxDataConfig, RequestPxMessage
from kl_site_server.utils import (
    SocketNamespace, socket_join_room, socket_leave_room, socket_send_to_session,
    to_socket_message_px_data_list,
)


def register_handlers_market_px(client: TouchanceDataClient):
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
    async def on_market_px_unsubscribe(session_id: str, message: str):
        subscription_message = MarketPxSubscriptionMessage.from_message(message)

        print_socket_event(
            MarketPxSocketEvent.UNSUBSCRIBE,
            session_id=session_id, namespace=namespace, additional=subscription_message.security,
        )

        socket_leave_room(session_id, subscription_message.security, namespace)

    @fast_api_socket.on(MarketPxSocketEvent.REQUEST, namespace=namespace)
    async def on_history_px_request(session_id: str, message: str):
        request_message = RequestPxMessage.from_message(message)

        print_socket_event(
            MarketPxSocketEvent.REQUEST,
            session_id=session_id, namespace=namespace, additional=f"[yellow]{request_message.identifier}[/yellow]",
        )

        try:
            get_active_user_by_oauth2_token(request_message.token)

            await socket_send_to_session(
                MarketPxSocketEvent.REQUEST,
                to_socket_message_px_data_list(client.get_px_data(
                    PxDataConfig.from_unique_identifiers([request_message.identifier])
                )),
                session_id
            )
        except HTTPException as ex:
            await socket_send_to_session(PxSocketEvent.SIGN_IN, ex.detail, session_id)
