from typing import TYPE_CHECKING

from pandas import DataFrame

from kl_site_server.calc import calc_pool
from tcoreapi_mq.message import RealtimeData
from .const import SYMBOL_NAMES
from .model import PxData

if TYPE_CHECKING:
    from kl_site_server.model import BarDataDict


class PxDataPool:
    def __init__(
        self, *,
        symbol: str,
        bars: list["BarDataDict"],
        min_tick: float,
        latest_market: RealtimeData,
    ):
        if not bars:
            raise ValueError(f"PxData should be initialized with data ({symbol} @ Pool)")

        if symbol not in SYMBOL_NAMES:
            raise ValueError(f"Symbol `{symbol}` doesn't have corresponding override name set")

        self.symbol: str = symbol
        self.symbol_name: str = SYMBOL_NAMES[symbol]
        self.min_tick: float = min_tick
        self.latest_market: RealtimeData | None = latest_market

        self.dataframe: DataFrame = DataFrame(bars)
        self.dataframe = calc_pool(self.dataframe, self.symbol)

    def to_px_data(self, period_min: int) -> PxData:
        return PxData(pool=self, period_min=period_min)
