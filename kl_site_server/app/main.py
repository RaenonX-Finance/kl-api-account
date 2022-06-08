from datetime import datetime, timedelta

from kl_site_server.client import TouchanceDataClient
from kl_site_server.model import TouchancePxRequestParams
from tcoreapi_mq.model import configs_sources_as_symbols

from .socket import register_handlers


latest_date: datetime = datetime.now() + timedelta(days=1)


def start_server_app():
    client = TouchanceDataClient()

    client.start()

    for symbol_obj in configs_sources_as_symbols():
        params = TouchancePxRequestParams(
            symbol_obj=symbol_obj,
            period_mins=[1, 5],
            history_range=(latest_date - timedelta(days=2), latest_date),
        )

        client.request_px_data(params)

    register_handlers(client)
