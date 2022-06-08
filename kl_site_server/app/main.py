from datetime import datetime

from kl_site_server.client import TouchanceDataClient
from tcoreapi_mq.model import configs_sources_as_symbols

from .socket import register_handlers


def start_server_app():
    client = TouchanceDataClient()

    client.start()

    for symbol in configs_sources_as_symbols():
        client.request_px_data(symbol, [60, 300], (datetime(2022, 5, 18), datetime(2022, 5, 19)))

    register_handlers(client)
