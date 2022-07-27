import numpy as np
from pandas import DataFrame

from typing import TYPE_CHECKING

from kl_site_common.const import DATA_SEGMENT_COUNT
from kl_site_server.utils import MAX_PERIOD
from .candlestick import calc_candlestick
from .diff import calc_diff
from .ema import calc_ema
from .index import calc_set_epoch_index
from .tie_point import calc_tie_point

if TYPE_CHECKING:
    from kl_site_server.model import PxDataConfig


def calc_model(df: DataFrame, px_data_config: "PxDataConfig") -> DataFrame:
    # Get the data rows to be calculated plus the period count of data needed
    # > Use `offset_num` for starting window to avoid math error (`number - None`)
    tail_start = -DATA_SEGMENT_COUNT - MAX_PERIOD - px_data_config.offset_num
    # > Use `None` for ending window if no offset (`0` != `None`)
    tail_end = -px_data_config.offset if px_data_config.offset else None
    df = df.iloc[tail_start:tail_end].copy()

    df = calc_diff(df)
    df = calc_candlestick(df)
    df = calc_ema(df)
    df = calc_tie_point(df, px_data_config.period_min)

    # Remove NaNs
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Tail again to keep calculated data only
    df = df.tail(DATA_SEGMENT_COUNT)

    return df


def calc_pool(df_1k: DataFrame) -> DataFrame:
    calc_set_epoch_index(df_1k)

    # Remove NaNs
    df_1k = df_1k.fillna(np.nan)

    return df_1k
