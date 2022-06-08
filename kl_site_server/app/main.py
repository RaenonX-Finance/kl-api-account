from datetime import datetime, timedelta

from kl_site_server.client import TouchanceDataClient
from tcoreapi_mq.model import configs_sources_as_symbols

from .socket import register_handlers


latest_date: datetime = datetime(2022, 6, 8)


def start_server_app():
    client = TouchanceDataClient()

    client.start()

    for symbol in configs_sources_as_symbols():
        client.request_px_data(symbol, [1, 5], (latest_date - timedelta(days=1), latest_date))

    register_handlers(client)
