from dataclasses import dataclass
from datetime import datetime
from math import ceil
from typing import Iterable, TYPE_CHECKING

from pandas.tseries.offsets import BDay

if TYPE_CHECKING:
    from kl_site_server.db import UserConfigModel
    from kl_site_server.model import BarDataDict, PxDataCommon, RequestPxMessageSingle
    from kl_site_server.endpoints import RequestPxMessageSingle as RequestPxMessageSingleModel


@dataclass(kw_only=True, frozen=True, order=True)
class PxDataConfig:
    security: str
    period_min: int
    offset: int | None
    limit: int | None

    @property
    def earliest_ts(self) -> datetime:
        return (datetime.utcnow() - BDay(ceil(self.period_min / 1440))).to_pydatetime()

    @property
    def offset_num(self) -> int:
        return self.offset or 0

    @staticmethod
    def from_request_px_message(requests: Iterable["RequestPxMessageSingle"]) -> set["PxDataConfig"]:
        ret = set()

        for request in requests:
            identifier = request["identifier"]
            offset = request.get("offset")
            limit = request.get("limit")

            security, period_min = identifier.split("@", 1)
            ret.add(PxDataConfig(security=security, period_min=int(period_min), offset=offset, limit=limit))

        return ret

    @staticmethod
    def from_request_px_message_model(requests: Iterable["RequestPxMessageSingleModel"]) -> set["PxDataConfig"]:
        ret = set()

        for request in requests:
            identifier = request.identifier
            offset = request.offset
            limit = request.limit

            security, period_min = identifier.split("@", 1)
            ret.add(PxDataConfig(security=security, period_min=int(period_min), offset=offset, limit=limit))

        return ret

    @staticmethod
    def from_unique_identifiers(
        identifiers: Iterable[str], *,
        securities_to_include: Iterable[str] | None = None
    ) -> set["PxDataConfig"]:
        ret = set()

        for identifier in identifiers:
            security, period_min = identifier.split("@", 1)

            if securities_to_include and security not in securities_to_include:
                continue

            ret.add(PxDataConfig(security=security, period_min=int(period_min), offset=None, limit=None))

        return ret

    @classmethod
    def from_config(cls, config: "UserConfigModel") -> set["PxDataConfig"]:
        return cls.from_unique_identifiers(config.slot_map.values())

    def __str__(self):
        return f"{self.security}@{self.period_min}"

    def __repr__(self):
        return str(self)


class PxData:
    def __init__(
        self, *,
        common: "PxDataCommon",
        px_data_config: PxDataConfig,
        calculated_data: list["BarDataDict"],
    ):
        self.common: "PxDataCommon" = common

        self.data: list["BarDataDict"] = calculated_data

        self.period_min: int = px_data_config.period_min
        self.offset: int | None = px_data_config.offset

    def get_current(self) -> "BarDataDict":
        return self.get_last_n(1)

    def get_last_n(self, n: int) -> "BarDataDict":
        return self.data[-n]

    @property
    def unique_identifier(self) -> str:
        return f"{self.common.security}@{self.period_min}"
