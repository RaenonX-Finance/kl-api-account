from dataclasses import dataclass
from typing import Generator

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
        df[PxDataCol.DATE_MARKET] + BDay(1),
        df[PxDataCol.DATE_MARKET] + BDay(0),
    )

    return df


def _calc_key_time(df_1k: DataFrame, key_timestamps: KeyTimestamps) -> DataFrame:
    open_ts, close_ts = key_timestamps.open, key_timestamps.close

    day_open = _get_bool_series_at_time(df_1k, open_ts)
    day_close = _get_bool_series_at_time(df_1k, close_ts)

    return _calc_key_time_add_group_basis(df_1k[day_open | day_close].copy(), close_ts)


_TS_FUT_US: KeyTimestamps = KeyTimestamps(open="13:30", close="20:00")

_TS_FUT_TW_MAIN: KeyTimestamps = KeyTimestamps(open="00:45", close="05:30")

_TS_FUT_TW_BASIC: KeyTimestamps = KeyTimestamps(open="07:00", close="05:30")

_KEY_TS_MAP: dict[str, KeyTimestamps] = {
    "NQ": _TS_FUT_US,
    "YM": _TS_FUT_US,
    "FITX": _TS_FUT_TW_MAIN,
}


def _sr_levels_range_of_pair(
    df_selected: DataFrame, *,
    sort_grouped_levels: bool,
    group_basis: str,
) -> Generator[list[float], None, None]:
    values_dict = df_selected.groupby(group_basis)[PxDataCol.OPEN].apply(list).to_dict()

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

        if sort_grouped_levels:
            yield sorted(levels_group)
        else:
            yield levels_group


def sr_levels_range_of_pair(df_1k: DataFrame, symbol: str) -> list[list[float]]:
    if key_timestamp := _KEY_TS_MAP.get(symbol):
        df_selected = _calc_key_time(df_1k, key_timestamp)
    else:
        raise ValueError(f"Symbol `{symbol}` does not have key time picking logic")

    levels: list[list[float]] = []
    sr_level_groups = _sr_levels_range_of_pair(
        df_selected,
        sort_grouped_levels=True,
        group_basis=PxDataCol.AUTO_SR_GROUP_BASIS,
    )

    for levels_group in sr_level_groups:
        levels.append(levels_group)

    return levels


def sr_levels_range_of_pair_merged(df_1k: DataFrame, symbol: str) -> list[float]:
    if symbol != "FITX":
        return []

    df_selected = _calc_key_time(df_1k, _TS_FUT_TW_BASIC)

    levels: list[float] = []
    sr_level_groups = _sr_levels_range_of_pair(
        df_selected,
        sort_grouped_levels=False,
        group_basis=PxDataCol.DATE_MARKET,
    )

    for levels_group in sr_level_groups:
        levels.extend(levels_group)

    return sorted(levels)
