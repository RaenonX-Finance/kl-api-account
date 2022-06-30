import json
from typing import TypeAlias, TypedDict

from kl_site_common.const import SR_CUSTOM_LEVELS


class CustomSrLevel(TypedDict):
    level: float


CustomSrLevelDict: TypeAlias = dict[str, list[CustomSrLevel]]


class InitData(TypedDict):
    customSrLevelDict: CustomSrLevelDict


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


def to_socket_message_init_data() -> str:
    data: InitData = {
        "customSrLevelDict": _to_custom_sr_level_dict(),
    }

    return json.dumps(data)
