from typing import TYPE_CHECKING

from kl_site_server.const import fast_api
from kl_site_server.endpoints import admin_router, auth_router, generate_px_router, user_router

if TYPE_CHECKING:
    from kl_site_server.client import TouchanceDataClient


def register_api_routes(client: "TouchanceDataClient"):
    fast_api.include_router(admin_router)
    fast_api.include_router(auth_router)
    fast_api.include_router(generate_px_router(client))
    fast_api.include_router(user_router)
