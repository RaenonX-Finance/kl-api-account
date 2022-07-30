from ._base import SymbolBase
from .types import FuturesExpiry


class FuturesSymbol(SymbolBase):
    def __init__(self, *, exchange: str, symbol: str, expiry: FuturesExpiry = "HOT"):
        self.exchange = exchange
        self.symbol_ = symbol
        self.expiry = expiry

    @property
    def security(self) -> str:
        return self.symbol_

    @property
    def symbol_complete(self) -> str:
        return f"TC.F.{self.exchange}.{self.security}.{self.expiry}"
