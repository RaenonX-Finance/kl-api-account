from typing import Literal, TypeAlias, TypeVar, TypedDict

from ._base import SymbolBase

SymbolBaseType = TypeVar("SymbolBaseType", bound=SymbolBase)

DataSourceProductType: TypeAlias = Literal["Futures"]

FuturesExpiry: TypeAlias = Literal["HOT", "HOT2"]


class DataSourceConfigEntry(TypedDict):
    type: DataSourceProductType
    exchange: str
    symbol: str
    expiry: FuturesExpiry
