from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any

from bson import ObjectId
from pydantic import BaseModel, Field, root_validator, validator

from kl_site_common.db import PyObjectId
from kl_site_common.utils import time_hhmm_to_utc_time
from tcoreapi_mq.message import PxHistoryDataEntry


@dataclass(kw_only=True)
class DbHistoryDataResult:
    data: list[PxHistoryDataEntry]
    earliest: datetime | None
    latest: datetime | None

    def update_latest(self, entry: PxHistoryDataEntry) -> "DbHistoryDataResult":
        if entry.timestamp == self.data[-1].timestamp:
            self.data[-1] = entry
        else:
            self.data.append(entry)

        self.latest = entry.timestamp

        return self


@dataclass
class MarketSessionEntry:
    # Valid values of https://docs.python.org/3/library/datetime.html#datetime.date.weekday
    weekdays: list[0 | 1 | 2 | 3 | 4 | 5 | 6]
    start: time
    end: time

    @property
    def is_now_open(self) -> bool:
        now = datetime.utcnow()
        now_time = now.time().replace(tzinfo=timezone.utc)
        now_weekday = now.weekday()

        if self.end < self.start:
            # Cross day
            if now_weekday in self.weekdays and now_time > self.start:
                return True

            if (now_weekday - 1) % 7 in self.weekdays and now_time < self.end:
                return True

            return False

        return now_weekday in self.weekdays and self.start < now_time < self.end

    @staticmethod
    def from_config(config_obj: dict[str, Any]) -> "MarketSessionEntry":
        return MarketSessionEntry(
            weekdays=config_obj["weekdays"],
            start=time_hhmm_to_utc_time(config_obj["start"]),
            end=time_hhmm_to_utc_time(config_obj["end"]),
        )


class FuturesMarketClosedSessionModel(BaseModel):
    id: PyObjectId | None = Field(default_factory=PyObjectId, alias="_id")
    security: str = Field(..., description="Security name of the session, such as NQ.")
    start: datetime
    end: datetime

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

    @validator("start", "end")
    def check_tz_awareness(cls, v: datetime):
        if v.tzinfo is None:
            raise ValueError("`datetime` provided must be tz-aware")

        return v

    @validator("end")
    def check_end_later_than_now(cls, v: datetime):
        if v < datetime.utcnow().replace(tzinfo=timezone.utc):
            raise ValueError("End time should be later than now")

        return v

    @root_validator
    def check_end_later_than_start(cls, values: dict[str, Any]) -> dict[str, Any]:
        start = values.get("start")
        end = values.get("end")

        if start > end:
            raise ValueError("`end` should be later than `start`")

        return values

    @property
    def is_now_closed(self) -> bool:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)

        return self.start < now < self.end
