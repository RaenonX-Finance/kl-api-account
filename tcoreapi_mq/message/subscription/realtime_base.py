from abc import ABC, abstractmethod
from datetime import datetime, time

from ._base import SubscriptionDataBase


class RealtimeDataBase(SubscriptionDataBase, ABC):
    @property
    @abstractmethod
    def is_valid(self) -> bool:
        raise NotImplementedError()

    @property
    @abstractmethod
    def symbol_complete(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def security(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def last_px(self) -> float:
        raise NotImplementedError()

    @property
    @abstractmethod
    def open(self) -> float:
        raise NotImplementedError()

    @property
    @abstractmethod
    def high(self) -> float:
        raise NotImplementedError()

    @property
    @abstractmethod
    def low(self) -> float:
        raise NotImplementedError()

    @property
    @abstractmethod
    def close(self) -> float:
        raise NotImplementedError()

    @property
    @abstractmethod
    def change_val(self) -> float:
        raise NotImplementedError()

    @property
    def change_pct(self) -> float:
        return self.change_val / self.open * 100

    @property
    @abstractmethod
    def filled_datetime(self) -> datetime:
        raise NotImplementedError()

    @property
    @abstractmethod
    def filled_time(self) -> time:
        raise NotImplementedError()
