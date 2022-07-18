from typing import TypedDict


class ProductInfo(TypedDict):
    name: str
    symbol: str


class PeriodInfo(TypedDict):
    min: int
    name: str
