import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from kl_site_common.utils import print_log
from kl_site_server.model import PxDataConfig
from kl_site_server.utils import generate_bad_request_exception, to_api_response_px_data_list
from .model import PxInitParams, RequestPxParams
from ..auth import get_active_user_by_oauth2_token

if TYPE_CHECKING:
    from kl_site_server.client import TouchanceDataClient


def generate_px_router(client: "TouchanceDataClient") -> APIRouter:
    px_router = APIRouter(prefix="/px")

    @px_router.post(
        "/init",
        description="Get the px data for frontend initialization.",
        # Not using `response_model` here as it filters out `ema*` keys in `PxDataBar`
    )
    async def get_init_px_data(body: PxInitParams = Body(...)) -> JSONResponse:
        _start = time.time()

        token = body.token
        identifiers = body.identifiers

        if not identifiers:
            raise generate_bad_request_exception("`identifiers` cannot be empty when initializing px data")

        # Ensure the `token` is valid and the user is active
        get_active_user_by_oauth2_token(token)

        px_data_configs = PxDataConfig.from_unique_identifiers(identifiers)
        px_data_list = to_api_response_px_data_list(client.get_px_data(px_data_configs))

        print_log(
            "Sending Px initialization data of "
            f"({' / '.join(f'[yellow]{config}[/]' for config in px_data_configs)} - {time.time() - _start:.3f} s)"
        )

        return JSONResponse(content=px_data_list)

    @px_router.post(
        "/request",
        description="Request history px data.",
        # Not using `response_model` here as it filters out `ema*` keys in `PxDataBar`
    )
    async def handle_px_data_request(body: RequestPxParams = Body(...)) -> JSONResponse:
        _start = time.time()

        token = body.token
        requests = body.requests

        get_active_user_by_oauth2_token(token)

        px_data_configs = PxDataConfig.from_request_px_message_model(requests)
        px_data_list = to_api_response_px_data_list(client.get_px_data(px_data_configs))

        print_log(
            "Sending Px data of "
            f"({' / '.join(f'[yellow]{config}[/]' for config in px_data_configs)} - {time.time() - _start:.3f} s)"
        )

        return JSONResponse(content=px_data_list)

    return px_router
