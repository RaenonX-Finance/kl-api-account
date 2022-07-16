from typing import TypeAlias, TypedDict

from kl_site_common.const import SR_CUSTOM_LEVELS
from kl_site_server.endpoints import UserConfigModel
from .utils import dump_and_compress


class CustomSrLevel(TypedDict):
    level: float


CustomSrLevelDict: TypeAlias = dict[str, list[CustomSrLevel]]


class InitData(TypedDict):
    customSrLevelDict: CustomSrLevelDict
    config: UserConfigModel


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


def to_socket_message_init_data(config: UserConfigModel) -> bytes:
    data: InitData = {
        "customSrLevelDict": _to_custom_sr_level_dict(),
        "config": config,
    }

    return dump_and_compress(data)
