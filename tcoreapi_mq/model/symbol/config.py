from tcoreapi_mq.const import DATA_SOURCES

from .futures import FuturesSymbol
from .types import DataSourceConfigEntry


DATA_SOURCES: list[DataSourceConfigEntry]


def configs_sources_as_symbols() -> list[FuturesSymbol]:
    return [
        FuturesSymbol(exchange=entry["exchange"], symbol=entry["symbol"], expiry=entry["expiry"])
        for entry in DATA_SOURCES
    ]
