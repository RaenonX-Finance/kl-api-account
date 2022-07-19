from kl_site_server.client import TouchanceDataClient
from .general import register_handlers_general
from .px import register_handlers_px


def register_handlers(client: TouchanceDataClient):
    register_handlers_general(client)
    register_handlers_px(client)
