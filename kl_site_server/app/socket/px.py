import asyncio
import time

from fastapi import HTTPException

from kl_site_common.utils import print_error, print_socket_event
from kl_site_server.client import TouchanceDataClient
from kl_site_server.const import fast_api_socket
from kl_site_server.endpoints import get_active_user_by_oauth2_token
from kl_site_server.enums import PxSocketEvent
from kl_site_server.model import MarketPxSubscriptionMessage, PxDataConfig, PxInitMessage, RequestPxMessage
from kl_site_server.utils import (
    SocketNamespace, make_px_data_room_name, make_px_sub_room_name, socket_join_room,
    socket_leave_room,
    socket_send_to_session, to_socket_message_px_data_list,
)
from .utils import get_tasks_with_session_control, on_http_exception


def register_handlers_px(client: TouchanceDataClient):
    namespace: SocketNamespace = "/px"

    @fast_api_socket.on(PxSocketEvent.PX_INIT, namespace=namespace)
    async def on_request_px_data_init(session_id: str, init_message: PxInitMessage):
        _start = time.time()

        if not init_message["identifiers"]:
            print_error("[Socket] `identifiers` cannot be empty for socket event `pxInit`")
            return

        px_data_config = set()

        try:
            user_data = get_active_user_by_oauth2_token(init_message["token"])
            px_data_config = PxDataConfig.from_unique_identifiers(init_message["identifiers"])

            await asyncio.gather(
                *get_tasks_with_session_control(user_data.id, namespace, session_id),
                socket_send_to_session(
                    PxSocketEvent.PX_INIT,
                    to_socket_message_px_data_list(client.get_px_data(px_data_config)),
                    session_id,
                    namespace=namespace
                )
            )
        except HTTPException as ex:
            await on_http_exception(ex, session_id, namespace)
        finally:
            print_socket_event(
                PxSocketEvent.PX_INIT,
                session_id=session_id,
                namespace=namespace,
                additional=(
                    f"{time.time() - _start:.3f} s "
                    f"({' / '.join(f'[yellow]{config}[/yellow]' for config in px_data_config)})"
                )
            )

    @fast_api_socket.on(PxSocketEvent.SUBSCRIBE, namespace=namespace)
    async def on_market_px_subscribe(session_id: str, subscription_message: MarketPxSubscriptionMessage):
        identifiers = subscription_message["identifiers"]
        print_socket_event(
            PxSocketEvent.SUBSCRIBE,
            session_id=session_id, namespace=namespace, additional=f"[yellow]{identifiers}[/yellow]",
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
            session_id=session_id, namespace=namespace, additional=f"[yellow]{identifiers}[/yellow]",
        )

        socket_leave_room(session_id, make_px_sub_room_name(identifiers), namespace)
        socket_leave_room(session_id, make_px_data_room_name(identifiers), namespace)

    @fast_api_socket.on(PxSocketEvent.REQUEST, namespace=namespace)
    async def on_history_px_request(session_id: str, request_message: RequestPxMessage):
        _start = time.time()

        try:
            get_active_user_by_oauth2_token(request_message["token"])

            px_data_list_message = to_socket_message_px_data_list(client.get_px_data(
                PxDataConfig.from_request_px_message(request_message["requests"])
            ))

            await socket_send_to_session(
                PxSocketEvent.REQUEST,
                px_data_list_message,
                session_id,
                namespace=namespace
            )
        except HTTPException as ex:
            await on_http_exception(ex, session_id, namespace)
        finally:
            print_socket_event(
                PxSocketEvent.REQUEST,
                session_id=session_id, namespace=namespace,
                additional=f"{time.time() - _start:.3f} s / [yellow]{request_message['requests']}[/yellow]",
            )
