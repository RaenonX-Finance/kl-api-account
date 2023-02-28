from kl_api_account.const import fast_api
from kl_api_account.endpoints import admin_router, auth_router, user_router


def register_api_routes():
    fast_api.include_router(admin_router)
    fast_api.include_router(auth_router)
    fast_api.include_router(user_router)
