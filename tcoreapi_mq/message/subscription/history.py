from dataclasses import dataclass
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
    def symbol_complete(self) -> str:
        return self.data.body["Symbol"]

    @property
    def status(self) -> str:
        return self.data.body["Status"]

    @property
    def is_ready(self):
        return self.status == "Ready"


@dataclass(kw_only=True)
class HistoryData:
    data_list: list["PxHistoryDataEntry"]
    handshake: HistoryDataHandshake

    @property
    def is_1k(self) -> bool:
        return self.handshake.data_type == "1K"

    @property
    def is_dk(self) -> bool:
        return self.handshake.data_type == "DK"
