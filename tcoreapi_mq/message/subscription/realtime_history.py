from datetime import datetime, time, timezone

from tcoreapi_mq.model import FUTURES_SYMBOL_TO_SYM_OBJ
from .common import CommonData
from .history import HistoryData, HistoryDataHandshake
from .realtime_base import RealtimeDataBase
from ..receive import PxHistoryDataEntry


class RealtimeDataHistory(RealtimeDataBase):
    def __init__(self, data: HistoryData, handshake: HistoryDataHandshake):
        super().__init__(CommonData(handshake.data.text))

        self._data: HistoryData = data
        self._last_bar: PxHistoryDataEntry = data.data_list[-1]
        self._security: str = FUTURES_SYMBOL_TO_SYM_OBJ[self.symbol_complete].security

    @property
    def is_valid(self) -> bool:
        return True

    @property
    def symbol_complete(self) -> str:
        return self._data.symbol_complete

    @property
    def security(self) -> str:
        return self._security

    @property
    def last_px(self) -> float:
        return self._last_bar.close

    @property
    def open(self) -> float:
        return self._last_bar.open

    @property
    def high(self) -> float:
        return self._last_bar.high

    @property
    def low(self) -> float:
        return self._last_bar.low

    @property
    def close(self) -> float:
        return self._last_bar.close

    @property
    def change_val(self) -> float:
        return self.close - self.open

    @property
    def filled_datetime(self) -> datetime:
        return datetime.utcnow().replace(tzinfo=timezone.utc)

    @property
    def filled_time(self) -> time:
        return datetime.utcnow().time().replace(tzinfo=timezone.utc)
