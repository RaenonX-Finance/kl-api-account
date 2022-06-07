from abc import ABC

from .common import CommonData


class SubscriptionDataBase(ABC):
    def __init__(self, data: CommonData):
        self.data = data
