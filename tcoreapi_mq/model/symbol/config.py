from kl_site_common.const import DATA_SOURCES

from .futures import FuturesSymbol


SOURCE_SYMBOLS: list[FuturesSymbol] = [
    FuturesSymbol(exchange=entry["exchange"], symbol=entry["symbol"], expiry=entry["expiry"])
    for entry in DATA_SOURCES
]

COMPLETE_SYMBOL_TO_SYM_OBJ: dict[str, FuturesSymbol] = {entry.symbol_complete: entry for entry in SOURCE_SYMBOLS}
