from dataclasses import dataclass

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.tseries.offsets import BDay

from kl_site_common.const import SR_LEVEL_MIN_DIFF
from kl_site_server.enums import PxDataCol


@dataclass(kw_only=True)
class KeyTimestamps:
    open: str
    close: str


def _get_bool_series_at_time(df: DataFrame, close_ts: str) -> Series:
    return df[PxDataCol.DATE].dt.time == pd.to_datetime(close_ts).time()


def _calc_key_time_add_group_basis(df: DataFrame, ts: str) -> DataFrame:
    df[PxDataCol.AUTO_SR_GROUP_BASIS] = np.where(
        _get_bool_series_at_time(df, ts),
        df[PxDataCol.DATE_MARKET].dt.date + BDay(1),
        df[PxDataCol.DATE_MARKET].dt.date + BDay(0),
    )

    return df


def _calc_key_time(df_1k: DataFrame, key_timestamps: KeyTimestamps) -> DataFrame:
    open_ts, close_ts = key_timestamps.open, key_timestamps.close

    day_open = _get_bool_series_at_time(df_1k, open_ts)
    day_close = _get_bool_series_at_time(df_1k, close_ts)

    return _calc_key_time_add_group_basis(df_1k[day_open | day_close].copy(), close_ts)


_TS_FUT_US: KeyTimestamps = KeyTimestamps(open="13:30", close="20:00")

_TS_FUT_TW_MAIN: KeyTimestamps = KeyTimestamps(open="00:45", close="05:30")

_KEY_TS_MAP: dict[str, KeyTimestamps] = {
    "NQ": _TS_FUT_US,
    "YM": _TS_FUT_US,
    "FITX": _TS_FUT_TW_MAIN,
}


def support_resistance_range_of_2_close(df_1k: DataFrame, symbol: str) -> list[list[float]]:
    if key_timestamp := _KEY_TS_MAP.get(symbol):
        df_selected = _calc_key_time(df_1k, key_timestamp)
    else:
        raise ValueError(f"Symbol `{symbol}` does not have key time picking logic")

    values_dict = df_selected.groupby(PxDataCol.AUTO_SR_GROUP_BASIS)[PxDataCol.OPEN].apply(list).to_dict()
    levels: list[list[float]] = []

    for level_pair in values_dict.values():
        if len(level_pair) != 2:
            continue  # Pair incomplete - skip

        higher = max(level_pair)
        lower = min(level_pair)

        diff = higher - lower

        levels_group = []

        if diff >= SR_LEVEL_MIN_DIFF:
            levels_group.extend([lower - diff * diff_mult for diff_mult in range(8)])
            levels_group.extend([higher + diff * diff_mult for diff_mult in range(8)])

        levels.append(sorted(levels_group))

    return levels
