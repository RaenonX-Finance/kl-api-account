from datetime import datetime, timedelta
from typing import Type

from kl_site_common.utils import print_log
from kl_site_server.client import TouchanceDataClient
from kl_site_server.model import TouchancePxRequestParams
from tcoreapi_mq.model import configs_sources_as_symbols

from .socket import register_handlers


latest_date: datetime = datetime.utcnow() + timedelta(hours=1)


def start_server_app(client_cls: Type[TouchanceDataClient] | None = TouchanceDataClient):
    client = client_cls()

    client.start()

    for symbol_obj in configs_sources_as_symbols():
        print_log(f"[Server] Requesting Px data of [yellow]{symbol_obj.symbol_complete}[/yellow]")
        params = TouchancePxRequestParams(
            symbol_obj=symbol_obj,
            period_mins=[1, 5],
            history_range=(latest_date - timedelta(days=15), latest_date),
        )

        client.request_px_data(params)

    register_handlers(client)
