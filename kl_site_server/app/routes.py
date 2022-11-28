from kl_site_server.const import fast_api
from kl_site_server.endpoints import admin_router, auth_router, user_router, webrtc_router


def register_api_routes():
    fast_api.include_router(admin_router)
    fast_api.include_router(auth_router)
    fast_api.include_router(user_router)
    fast_api.include_router(webrtc_router)
