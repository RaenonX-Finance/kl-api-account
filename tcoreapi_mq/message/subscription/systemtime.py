from datetime import datetime, timezone

from kl_site_common.utils import time_round_second_to_min
from ._base import SubscriptionDataBase
from .common import CommonData


class SystemTimeData(SubscriptionDataBase):
    def __init__(self, data: CommonData):
        super().__init__(data)

        self.timestamp: datetime = datetime.strptime(
            f"{self.data.body['Date']} {self.data.body['Time']:>06}",
            "%Y%m%d %H%M%S"
        ).replace(tzinfo=timezone.utc)
        self.timestamp: datetime = time_round_second_to_min(self.timestamp)
        self.epoch_sec: float = self.timestamp.timestamp()
