from typing import Literal, TypeAlias, TypeVar, TypedDict

from ._base import SymbolBase

SymbolBaseType = TypeVar("SymbolBaseType", bound=SymbolBase)

DataSourceProducType: TypeAlias = Literal["Futures"]

FuturesExpiry: TypeAlias = Literal["HOT", "HOT2"]


class DataSourceConfigEntry(TypedDict):
    type: DataSourceProducType
    exchange: str
    symbol: str
    expiry: FuturesExpiry
