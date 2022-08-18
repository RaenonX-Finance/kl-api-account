from datetime import datetime, timezone
from typing import Callable, Literal, TypeAlias

from pandas.tseries.offsets import BDay

from kl_site_common.const import DATA_SOURCES
from kl_site_common.utils import get_epoch_sec_time
from tcoreapi_mq.model import configs_sources_as_symbols

MarketDateType: TypeAlias = Literal[
    "US Index Futures",
    "FITX"
]


def _calc_market_date_us_index_futures(timestamp: datetime, epoch_sec_time: float) -> datetime:
    return timestamp.date() + BDay(0 if epoch_sec_time < get_epoch_sec_time(22) else 1)


def _calc_market_date_fitx(timestamp: datetime, epoch_sec_time: float) -> datetime:
    return timestamp.date() + BDay(0 if epoch_sec_time >= get_epoch_sec_time(0, 45) else -1)


_symbol_market_date_type_map: dict[str, MarketDateType] = {
    entry.symbol_complete: next(
        source for source in DATA_SOURCES
        if source["symbol"] == entry.symbol_
    )["type-market-date"]
    for entry in configs_sources_as_symbols()
}


_calc_function_map: dict[MarketDateType, Callable[[datetime, float], datetime]] = {
    "US Index Futures": _calc_market_date_us_index_futures,
    "FITX": _calc_market_date_fitx,
}


def calc_market_date(timestamp: datetime, epoch_sec_time: float, symbol_complete: str) -> datetime:
    if calc_market_date_symbol := _calc_function_map.get(_symbol_market_date_type_map[symbol_complete]):
        return calc_market_date_symbol(timestamp, epoch_sec_time).replace(tzinfo=timezone.utc)

    raise ValueError(f"Symbol `{symbol_complete}` does not have market date calculation logic")
