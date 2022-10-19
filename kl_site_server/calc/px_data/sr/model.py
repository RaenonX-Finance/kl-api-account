from dataclasses import InitVar, dataclass, field
from typing import Optional

from kl_site_common.utils import time_str_to_utc_time, time_to_total_seconds


@dataclass(kw_only=True)
class SrLevelKeyTimePair:
    open_str: InitVar[str]
    close_str: InitVar[str]

    open_time_sec: int = field(init=False)
    close_time_sec: int = field(init=False)

    def __post_init__(self, open_str: str, close_str: str):
        self.open_time_sec = time_to_total_seconds(time_str_to_utc_time(open_str))
        self.close_time_sec = time_to_total_seconds(time_str_to_utc_time(close_str))

    @staticmethod
    def from_config_obj(config_obj: dict[str, str] | None) -> Optional["SrLevelKeyTimePair"]:
        if not config_obj:
            return None

        return SrLevelKeyTimePair(open_str=config_obj["open"], close_str=config_obj["close"])


@dataclass(kw_only=True)
class SrLevelKeyTimes:
    group: SrLevelKeyTimePair
    basic: SrLevelKeyTimePair | None

    @staticmethod
    def from_config_obj(config_obj: dict[str, dict[str, str] | None]) -> "SrLevelKeyTimes":
        return SrLevelKeyTimes(
            group=SrLevelKeyTimePair.from_config_obj(config_obj["group"]),
            basic=SrLevelKeyTimePair.from_config_obj(config_obj.get("basic"))
        )


@dataclass(kw_only=True)
class SRLevelsData:
    groups: list[list[float]]
    basic: list[float]
