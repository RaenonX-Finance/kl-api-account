from typing import TypeAlias, TypedDict

from kl_site_common.const import DATA_SOURCES, SR_CUSTOM_LEVELS
from kl_site_server.db import PX_CONFIG, UserConfigModel
from .data import PeriodInfo, ProductInfo
from .utils import dump_and_compress


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
    period_min_info = [{"min": period.period_min, "name": period.name} for period in PX_CONFIG.period_mins]
    period_day_info = [{"min": period.period_min, "name": period.name} for period in PX_CONFIG.period_days]

    return period_min_info + period_day_info


def to_socket_message_init_data(config: "UserConfigModel") -> bytes:
    data: InitData = {
        "customSrLevelDict": _to_custom_sr_level_dict(),
        "config": config,
        "products": _to_products(),
        "periods": _to_periods(),
    }

    return dump_and_compress(data)
