from ._base import SymbolBase
from .types import FuturesExpiry


class FuturesSymbol(SymbolBase):
    def __init__(self, *, exchange: str, symbol: str, expiry: FuturesExpiry = "HOT"):
        self.exchange = exchange
        self.symbol = symbol
        self.expiry = expiry

    @property
    def symbol_name(self) -> str:
        return f"TC.F.{self.exchange}.{self.symbol}.{self.expiry}"
