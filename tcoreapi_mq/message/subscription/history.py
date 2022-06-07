from typing import TYPE_CHECKING

from ._base import SubscriptionDataBase

if TYPE_CHECKING:
    from tcoreapi_mq.message import DataType, PxHistoryDataEntry


class HistoryDataHandshake(SubscriptionDataBase):
    @property
    def data_type(self) -> "DataType":
        return self.data.data_type

    @property
    def start_time_str(self) -> str:
        return self.data.body["StartTime"]

    @property
    def end_time_str(self) -> str:
        return self.data.body["EndTime"]

    @property
    def symbol_name(self) -> str:
        return self.data.body["Symbol"]

    @property
    def status(self) -> str:
        return self.data.body["Status"]

    @property
    def is_ready(self):
        return self.status == "Ready"


class HistoryData:
    def __init__(self, data_list: list["PxHistoryDataEntry"]):
        self.data_list = data_list
