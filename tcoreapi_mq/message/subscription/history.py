from dataclasses import dataclass
from typing import Iterator, TYPE_CHECKING

from ._base import SubscriptionDataBase

if TYPE_CHECKING:
    from kl_site_server.db import DbHistoryDataResult
    from tcoreapi_mq.message import DataType, PxHistoryDataEntry, PxHistoryDataMongoModel


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
    data_iter: Iterator["PxHistoryDataEntry"]
    data_len: int | None
    data_type: "DataType"
    symbol_complete: str

    @staticmethod
    def from_socket_message(
        history_data: list["PxHistoryDataEntry"],
        handshake: HistoryDataHandshake
    ) -> "HistoryData":
        return HistoryData(
            data_iter=history_data,
            data_len=len(history_data),
            data_type=handshake.data_type,
            symbol_complete=handshake.symbol_complete,
        )

    @staticmethod
    def from_db_fetch(
        symbol_complete: str,
        data_type: "DataType",
        result: "DbHistoryDataResult"
    ) -> "HistoryData":
        return HistoryData(
            data_iter=result.data,
            data_len=None,
            data_type=data_type,
            symbol_complete=symbol_complete,
        )

    def to_db_entries(self) -> Iterator["PxHistoryDataMongoModel"]:
        return iter(data.to_mongo_doc() for data in self.data_iter)

    @property
    def is_1k(self) -> bool:
        return self.data_type == "1K"

    @property
    def is_dk(self) -> bool:
        return self.data_type == "DK"

    @property
    def data_len_as_str(self) -> str:
        if self.data_len is None:
            return "(?)"

        return str(self.data_len)
