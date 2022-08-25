from datetime import datetime
from typing import TYPE_CHECKING

from pandas import DataFrame

from kl_site_common.const import DATA_SOURCES
from kl_site_server.calc import calc_pool, calc_strength, calc_support_resistance_levels
from kl_site_server.enums import PxDataCol
from tcoreapi_mq.message import RealtimeData
from .model import PxData, PxDataConfig

if TYPE_CHECKING:
    from kl_site_server.model import BarDataDict


class PxDataPool:
    def __init__(
        self, *,
        security: str,
        bars: list["BarDataDict"],
        min_tick: float,
        decimals: int,
        latest_market: RealtimeData,
        interval_sec: int,
    ):
        if not bars:
            raise ValueError(f"PxData should be initialized with data ({security} @ Pool)")

        try:
            source_of_symbol = next(data_source for data_source in DATA_SOURCES if data_source["symbol"] == security)
        except StopIteration as ex:
            raise ValueError(f"Symbol `{security}` is not included in data source list") from ex

        self.security: str = security
        self.symbol_name: str = source_of_symbol["name"]
        self.min_tick: float = min_tick
        self.decimals: int = decimals
        self.interval_sec: int = interval_sec
        self.latest_market: RealtimeData | None = latest_market

        self.dataframe: DataFrame = calc_pool(DataFrame(bars))

        self.strength: int = calc_strength(self.dataframe[PxDataCol.CLOSE])
        self.sr_levels_data = calc_support_resistance_levels(self.dataframe, self.security)

    @property
    def earliest_time(self) -> datetime:
        return self.dataframe[PxDataCol.DATE].min()

    @property
    def latest_time(self) -> datetime:
        return self.dataframe[PxDataCol.DATE].max()

    def to_px_data(self, px_data_config: PxDataConfig) -> PxData:
        return PxData(pool=self, px_data_config=px_data_config)
