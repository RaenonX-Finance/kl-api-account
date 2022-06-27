from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
from pandas import DataFrame, Series

from kl_site_common.utils import print_log
from kl_site_server.calc import calc_support_resistance_levels
from kl_site_server.enums import PxDataCol
from .calc import aggregate_df, calc_diff, calc_sma

if TYPE_CHECKING:
    from kl_site_server.model import PxDataPool


class PxData:
    def _proc_df(self):
        self.dataframe = calc_sma(self.dataframe)
        self.dataframe = calc_diff(self.dataframe)

        # Remove NaNs
        self.dataframe = self.dataframe.fillna(np.nan).replace([np.nan], [None])

    def __init__(
            self, *,
            pool: "PxDataPool",
            period_min: int,
    ):
        self.pool: "PxDataPool" = pool
        self.period_min: int = period_min

        self.dataframe: DataFrame = aggregate_df(pool.dataframe, period_min)
        self._proc_df()

        self.sr_levels_data = calc_support_resistance_levels(self.dataframe)

    def get_current(self) -> Series:
        return self.get_last_n(1)

    def get_last_n(self, n: int) -> Series:
        return self.dataframe.iloc[-n]

    def save_to_file(self):
        file_path = f"data-{self.pool.symbol}@{self.period_min}.csv"
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
        return f"{self.pool.symbol}@{self.period_min}"
