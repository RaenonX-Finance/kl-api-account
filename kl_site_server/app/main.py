from datetime import datetime, timedelta
from typing import Type

from kl_site_common.const import DATA_DOWNLOAD_1K, DATA_DOWNLOAD_DK
from kl_site_common.utils import print_log
from kl_site_server.client import TouchanceDataClient
from kl_site_server.db import PX_CONFIG
from kl_site_server.model import TouchancePxRequestParams
from tcoreapi_mq.model import SOURCE_SYMBOLS
from .routes import register_api_routes
from .socket import register_handlers

latest_date: datetime = datetime.utcnow() + timedelta(hours=1)


def start_server_app(
    client_cls: Type[TouchanceDataClient] | None = TouchanceDataClient, *,
    period_mins: list[int] | None = None,
    period_days: list[int] | None = None
):
    client = client_cls()
    client.start()

    params_list: list[TouchancePxRequestParams] = []
    data_period_mins = [period.period_min for period in PX_CONFIG.period_mins]
    data_period_days = list({period.period_min // 1440 for period in PX_CONFIG.period_days})

    for symbol_obj in SOURCE_SYMBOLS:
        print_log(f"Queueing Px data requests of [yellow]{symbol_obj.security}[/]")

        params_list.append(TouchancePxRequestParams(
            symbol_obj=symbol_obj,
            period_mins=period_mins if period_mins is not None else data_period_mins,
            period_days=period_days if period_days is not None else data_period_days,
            history_range_1k=(latest_date - timedelta(days=DATA_DOWNLOAD_1K), latest_date),
            history_range_dk=(latest_date - timedelta(days=DATA_DOWNLOAD_DK), latest_date),
        ))

    print_log(
        "Sending Px data requests of "
        f"{' / '.join([f'[yellow]{params.symbol_obj.security}[/]' for params in params_list])}"
    )
    client.request_px_data(params_list, re_calc_data=True)

    register_handlers()
    register_api_routes(client)
