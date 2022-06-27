from typing import TYPE_CHECKING

import numpy as np
from pandas import DataFrame

from tcoreapi_mq.message import RealtimeData
from .calc import calc_market_date
from .const import SYMBOL_NAMES
from .model import PxData

if TYPE_CHECKING:
    from kl_site_server.model import BarDataDict


class PxDataPool:
    def _proc_df(self):
        self.dataframe = calc_market_date(self.dataframe)

        # Remove NaNs
        self.dataframe = self.dataframe.fillna(np.nan)

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
        self._proc_df()

    def to_px_data(self, period_min: int) -> PxData:
        return PxData(pool=self, period_min=period_min)
