from kl_site_server.client import TouchanceDataClient
from .market_px import register_handlers_market_px
from .px import register_handlers_px


def register_handlers(client: TouchanceDataClient):
    register_handlers_px(client)
    register_handlers_market_px(client)
