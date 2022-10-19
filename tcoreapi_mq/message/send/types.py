from typing import TypeAlias, Literal


InstrumentType: TypeAlias = Literal["Futures", "Options", "Stock"]

HistoryInterval: TypeAlias = Literal["TICKS", "1K", "DK"]
