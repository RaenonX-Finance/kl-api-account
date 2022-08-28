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
    @classmethod
    def hash_bar_data(cls, bar: "BarDataDict") -> int:
        return hash(tuple(bar.items()))

    @classmethod
    def get_hashed_bars(cls, bars: list["BarDataDict"]) -> dict[int, int]:
        return {bar[PxDataCol.EPOCH_SEC]: cls.hash_bar_data(bar) for bar in bars}

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

        self._bars_cache: dict[int, int] = self.get_hashed_bars(bars)

    @property
    def earliest_time(self) -> datetime:
        return self.dataframe[PxDataCol.DATE].min()

    @property
    def latest_time(self) -> datetime:
        return self.dataframe[PxDataCol.DATE].max()

    def update_data(self, updated_data: dict[int, "BarDataDict"], latest_market: RealtimeData | None):
        self.latest_market = latest_market

        to_update = set(updated_data.keys())
        for epoch_sec, bar in updated_data.items():
            if self._bars_cache.get(epoch_sec) != self.hash_bar_data(bar):
                continue

            to_update.remove(epoch_sec)

        if not to_update:
            return

        bars = [updated_data[epoch_sec] for epoch_sec in to_update]
        df_update = calc_pool(DataFrame(bars))
        self.dataframe.update(df_update)
        self._bars_cache.update(self.get_hashed_bars(bars))

    def to_px_data(self, px_data_config: PxDataConfig) -> PxData:
        return PxData(pool=self, px_data_config=px_data_config)
