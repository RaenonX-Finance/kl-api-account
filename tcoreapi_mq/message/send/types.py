from typing import TypeAlias, Literal


InstrumentType: TypeAlias = Literal["Futures", "Options", "Stock"]

HistoryInterval: TypeAlias = Literal["TICKS", "1K", "DK"]

INTERVAL_TO_SEC: dict[HistoryInterval, int] = {
    "1K": 60,
    "DK": 86400,
}
