from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import talib
from pandas import DataFrame, DatetimeIndex, Series, to_datetime

from kl_site_common.const import SMA_PERIODS
from kl_site_common.utils import print_log
from kl_site_server.calc import calc_support_resistance_levels
from kl_site_server.enums import PxDataCol

from .bar_data import BarDataDict
from .const import SYMBOL_NAMES


class PxData:
    def _proc_df_date(self):
        self.dataframe[PxDataCol.DATE] = to_datetime(
            self.dataframe[PxDataCol.EPOCH_SEC], utc=True, unit="s"
        ).dt.tz_convert("America/Chicago").dt.tz_localize(None)
        self.dataframe.set_index(DatetimeIndex(self.dataframe[PxDataCol.DATE]), inplace=True)

        self.dataframe[PxDataCol.DATE_MARKET] = to_datetime(np.where(
            self.dataframe[PxDataCol.DATE].dt.hour < 17,
            self.dataframe[PxDataCol.DATE].dt.date,
            self.dataframe[PxDataCol.DATE].dt.date + timedelta(days=1)
        ))

    def _proc_df_smas(self):
        for sma_period in SMA_PERIODS:
            self.dataframe[PxDataCol.get_sma_col_name(sma_period)] = talib.SMA(
                self.dataframe[PxDataCol.CLOSE],
                timeperiod=sma_period
            )

    def _proc_df_diff(self):
        self.dataframe[PxDataCol.DIFF] = self.dataframe[PxDataCol.CLOSE] - self.dataframe[PxDataCol.OPEN]

    def _proc_df_vwap(self):
        # Don't calculate VWAP if period is 3600s+ (meaningless)
        if self.period_min >= 3600:
            self.dataframe[PxDataCol.VWAP] = np.full(len(self.dataframe.index), np.nan)
        else:
            self.dataframe[PxDataCol.PRICE_TIMES_VOLUME] = np.multiply(
                self.dataframe[PxDataCol.CLOSE],
                self.dataframe[PxDataCol.VOLUME]
            )
            mkt_data_group = self.dataframe.groupby(PxDataCol.DATE_MARKET)
            self.dataframe[PxDataCol.VWAP] = np.divide(
                mkt_data_group[PxDataCol.PRICE_TIMES_VOLUME].transform(pd.Series.cumsum),
                mkt_data_group[PxDataCol.VOLUME].transform(pd.Series.cumsum),
            )

    def _proc_df(self):
        self._proc_df_date()
        self._proc_df_smas()
        self._proc_df_diff()
        self._proc_df_vwap()

        # Remove NaNs
        self.dataframe = self.dataframe.fillna(np.nan).replace([np.nan], [None])

    def __init__(
            self, *,
            symbol: str,
            bars: list[BarDataDict],
            min_tick: float,
            period_min: int,
    ):
        if not bars:
            raise ValueError(f"PxData should be initialized with data ({symbol} @ {period_min})")

        if symbol not in SYMBOL_NAMES:
            raise ValueError(f"Symbol `{symbol}` doesn't have corresponding override name set")

        self.symbol: str = symbol
        self.symbol_name: str = SYMBOL_NAMES[symbol]
        self.dataframe: DataFrame = DataFrame(bars)
        self.min_tick: float = min_tick
        self.period_min: int = period_min

        self._proc_df()

        self.market_dates: Series = self.dataframe[PxDataCol.DATE_MARKET].unique()
        self.sr_levels_data = calc_support_resistance_levels(self.dataframe)

    def get_current(self) -> Series:
        return self.get_last_n(1)

    def get_last_n(self, n: int) -> Series:
        return self.dataframe.iloc[-n]

    def save_to_file(self):
        file_path = f"data-{self.symbol}@{self.period_min}.csv"
        self.dataframe.to_csv(file_path)

        print_log(f"[yellow]Px data saved to {file_path}[/yellow]")

        return file_path

    @property
    def earliest_time(self) -> datetime:
        return self.dataframe[PxDataCol.DATE].min()

    @property
    def latest_time(self) -> datetime:
        return self.dataframe[PxDataCol.DATE].max()

    @property
    def current_close(self) -> float:
        return self.get_current()[PxDataCol.CLOSE]

    @property
    def unique_identifier(self) -> str:
        return f"{self.symbol}@{self.period_min}"
