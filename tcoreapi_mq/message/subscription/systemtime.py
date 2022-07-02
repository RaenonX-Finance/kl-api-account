from datetime import datetime

from ._base import SubscriptionDataBase
from .common import CommonData


class SystemTimeData(SubscriptionDataBase):
    def __init__(self, data: CommonData):
        super().__init__(data)

        self.timestamp = datetime.strptime(
            f"{self.data.body['Date']} {self.data.body['Time']:>06}",
            "%Y%m%d %H%M%S"
        )
