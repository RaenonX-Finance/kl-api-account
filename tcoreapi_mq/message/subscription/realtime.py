from decimal import Decimal

from ._base import SubscriptionDataBase
from .common import CommonData


class RealtimeData(SubscriptionDataBase):
    def __init__(self, data: CommonData):
        super().__init__(data)

        self.quote: dict[str, str] = self.data.body["Quote"]

    @property
    def symbol_name(self) -> str:
        return self.quote["Symbol"]

    @property
    def security(self) -> str:
        return self.quote["Security"]

    @property
    def exchange(self) -> str:
        return f"{self.quote['Exchange']} ({self.quote['ExchangeName']})"

    @property
    def last_px(self) -> Decimal:
        return Decimal(self.quote["TradingPrice"])
