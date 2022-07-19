import time

from fastapi import HTTPException

from kl_site_common.utils import print_socket_event
from kl_site_server.client import TouchanceDataClient
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_active_user_by_oauth2_token
from kl_site_server.enums import GeneralSocketEvent, PxSocketEvent
from kl_site_server.model import MarketPxSubscriptionMessage, PxDataConfig, RequestPxMessage
from kl_site_server.utils import (
    SocketNamespace, socket_join_room, socket_leave_room, socket_send_to_session,
    to_socket_message_px_data_list,
)


def register_handlers_px(client: TouchanceDataClient):
    namespace: SocketNamespace = "/px"

    @fast_api_socket.on(PxSocketEvent.SUBSCRIBE, namespace=namespace)
    async def on_market_px_subscribe(session_id: str, message: str):
        subscription_message = MarketPxSubscriptionMessage.from_message(message)

        print_socket_event(
            PxSocketEvent.SUBSCRIBE,
            session_id=session_id, namespace=namespace,
            additional=f"[yellow]{subscription_message.security}[/yellow]",
        )

        try:
            get_active_user_by_oauth2_token(subscription_message.token)
            socket_join_room(session_id, subscription_message.security, namespace)
        except HTTPException as ex:
            await socket_send_to_session(GeneralSocketEvent.SIGN_IN, ex.detail, session_id)

    @fast_api_socket.on(PxSocketEvent.UNSUBSCRIBE, namespace=namespace)
    async def on_market_px_unsubscribe(session_id: str, message: str):
        subscription_message = MarketPxSubscriptionMessage.from_message(message)

        print_socket_event(
            PxSocketEvent.UNSUBSCRIBE,
            session_id=session_id, namespace=namespace,
            additional=f"[yellow]{subscription_message.security}[/yellow]",
        )

        socket_leave_room(session_id, subscription_message.security, namespace)

    @fast_api_socket.on(PxSocketEvent.REQUEST, namespace=namespace)
    async def on_history_px_request(session_id: str, message: str):
        _start = time.time()
        request_message = RequestPxMessage.from_message(message)

        try:
            get_active_user_by_oauth2_token(request_message.token)

            px_data_list_message = to_socket_message_px_data_list(client.get_px_data(
                PxDataConfig.from_unique_identifiers([request_message.identifier])
            ))

            await socket_send_to_session(
                PxSocketEvent.REQUEST,
                px_data_list_message,
                session_id,
                namespace=namespace
            )
        except HTTPException as ex:
            await socket_send_to_session(GeneralSocketEvent.SIGN_IN, ex.detail, session_id)
        finally:
            print_socket_event(
                PxSocketEvent.REQUEST,
                session_id=session_id, namespace=namespace,
                additional=f"{time.time() - _start:.3f} s / [yellow]{request_message.identifier}[/yellow]",
            )
