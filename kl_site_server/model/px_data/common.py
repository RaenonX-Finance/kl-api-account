from kl_site_common.const import DATA_SOURCES
from kl_site_server.calc import CALC_STRENGTH_BARS_NEEDED, calc_strength, calc_support_resistance_levels
from kl_site_server.db import get_history_data_close_px_from_db
from tcoreapi_mq.message import RealtimeData
from tcoreapi_mq.model import FUTURES_SECURITY_TO_SYM_OBJ
from .model import PxData, PxDataConfig


class PxDataCommon:
    def __init__(
        self, *,
        security: str,
        min_tick: float,
        decimals: int,
        latest_market: RealtimeData | None,
        interval_sec: int,
        last_px: float,
    ):
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

        symbol_complete = FUTURES_SECURITY_TO_SYM_OBJ[security].symbol_complete

        self.strength: int = calc_strength(
            get_history_data_close_px_from_db(symbol_complete, CALC_STRENGTH_BARS_NEEDED)
        )
        self.sr_levels_data = calc_support_resistance_levels(self.security, last_px)

    def to_px_data(self, px_data_config: PxDataConfig, calculated_data: list[dict] | None) -> PxData:
        if not calculated_data:
            raise ValueError(f"`{self.security}` does not have calculated data available on PxData creation")

        return PxData(common=self, px_data_config=px_data_config, calculated_data=calculated_data)
