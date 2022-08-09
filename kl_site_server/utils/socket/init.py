from typing import TypeAlias, TypedDict, TYPE_CHECKING

from kl_site_common.const import DATA_PERIOD_DAYS, DATA_PERIOD_MINS, DATA_SOURCES, SR_CUSTOM_LEVELS
from .data import PeriodInfo, ProductInfo
from .utils import dump_and_compress

if TYPE_CHECKING:
    from kl_site_server.db import UserConfigModel


class CustomSrLevel(TypedDict):
    level: float


CustomSrLevelDict: TypeAlias = dict[str, list[CustomSrLevel]]


class InitData(TypedDict):
    customSrLevelDict: CustomSrLevelDict
    config: "UserConfigModel"
    products: list[ProductInfo]
    periods: list[PeriodInfo]


def _to_custom_sr_level_dict() -> CustomSrLevelDict:
    return {
        symbol: [
            {
                "level": sr_level["level"],
            }
            for sr_level in sr_levels
        ]
        for symbol, sr_levels in SR_CUSTOM_LEVELS.items()
    }


def _to_products() -> list[ProductInfo]:
    return [
        {"name": data_source["name"], "symbol": data_source["symbol"]}
        for data_source in DATA_SOURCES
    ]


def _to_periods() -> list[PeriodInfo]:
    period_min_info = [
        {"min": period_min_entry["min"], "name": period_min_entry["name"]}
        for period_min_entry in DATA_PERIOD_MINS
    ]
    period_day_info = [
        {"min": period_day_entry["day"] * 1440, "name": period_day_entry["name"]}
        for period_day_entry in DATA_PERIOD_DAYS
    ]

    return period_min_info + period_day_info


def to_socket_message_init_data(config: "UserConfigModel") -> bytes:
    data: InitData = {
        "customSrLevelDict": _to_custom_sr_level_dict(),
        "config": config,
        "products": _to_products(),
        "periods": _to_periods(),
    }

    return dump_and_compress(data)
