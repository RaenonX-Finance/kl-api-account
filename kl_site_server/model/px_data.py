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
        if self.period_sec >= 3600:
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
            period_sec: int,
    ):
        self.symbol: str = symbol
        self.dataframe: DataFrame = DataFrame(bars)
        self.min_tick: float = min_tick
        self.period_sec: int = period_sec

        self._proc_df()

        self.market_dates: Series = self.dataframe[PxDataCol.DATE_MARKET].unique()
        self.sr_levels_data = calc_support_resistance_levels(self.dataframe)

    def get_current(self) -> Series:
        return self.get_last_n(1)

    def get_last_n(self, n: int) -> Series:
        return self.dataframe.iloc[-n]

    def get_last_day_close(self) -> float | None:
        if len(self.market_dates) < 2:
            raise ValueError(
                f"Px data of {self.symbol} @ {self.period_sec} "
                f"only has a single market date: {self.market_dates}"
            )

        market_date_prev = self.market_dates[-2]

        last_day_df = self.dataframe[self.dataframe[PxDataCol.DATE_MARKET] == market_date_prev]

        if not len(last_day_df.index):
            return None

        last_day_last_entry = last_day_df.iloc[-1]
        return last_day_last_entry[PxDataCol.CLOSE]

    def get_today_open(self) -> float | None:
        market_date_prev = self.market_dates[-1]

        today_df = self.dataframe[self.dataframe[PxDataCol.DATE_MARKET] == market_date_prev]

        if not len(today_df.index):
            return None

        last_day_last_entry = today_df.iloc[0]
        return last_day_last_entry[PxDataCol.OPEN]

    def save_to_file(self):
        file_path = f"data-{self.symbol}@{self.period_sec}.csv"
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
        return f"{self.symbol}@{self.period_sec}"
