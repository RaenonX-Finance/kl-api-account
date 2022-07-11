import time
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, TypeAlias

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.tseries.offsets import BDay

from kl_site_common.const import SR_LEVEL_MIN_DIFF
from kl_site_common.utils import get_epoch_sec_time
from kl_site_server.enums import PxDataCol


@dataclass(kw_only=True)
class KeyTimestamps:
    open: int
    close: int


def _get_bool_series_at_time(df: DataFrame, epoch_sec_time: int) -> Series:
    return df[PxDataCol.EPOCH_SEC_TIME] == epoch_sec_time


def _calc_key_time_add_group_basis(df: DataFrame, epoch_sec_time: int) -> DataFrame:
    df[PxDataCol.AUTO_SR_GROUP_BASIS] = np.where(
        _get_bool_series_at_time(df, epoch_sec_time),
        df[PxDataCol.DATE_MARKET] + BDay(1),
        df[PxDataCol.DATE_MARKET] + BDay(0),
    )

    return df


def _calc_key_time(df_1k: DataFrame, key_timestamps: KeyTimestamps) -> DataFrame:
    open_epoch, close_epoch = key_timestamps.open, key_timestamps.close

    day_open = _get_bool_series_at_time(df_1k, open_epoch)
    day_close = _get_bool_series_at_time(df_1k, close_epoch)

    return _calc_key_time_add_group_basis(df_1k[day_open | day_close].copy(), close_epoch)


_TS_FUT_US: KeyTimestamps = KeyTimestamps(open=get_epoch_sec_time(13, 30), close=get_epoch_sec_time(20, 0))

_TS_FUT_TW_MAIN: KeyTimestamps = KeyTimestamps(open=get_epoch_sec_time(0, 45), close=get_epoch_sec_time(5, 30))

_TS_FUT_TW_BASIC: KeyTimestamps = KeyTimestamps(open=get_epoch_sec_time(7, 0), close=get_epoch_sec_time(5, 30))

_KEY_TS_MAP: dict[str, KeyTimestamps] = {
    "NQ": _TS_FUT_US,
    "YM": _TS_FUT_US,
    "FITX": _TS_FUT_TW_MAIN,
}

SrLevelDataPair: TypeAlias = [dict[str, float], dict[str, float]]


def _sr_levels_get_recent_n_only(
    grouped_dict: dict[datetime, SrLevelDataPair],
    count: int
) -> list[tuple[datetime, SrLevelDataPair]]:
    ret: list[tuple[datetime, SrLevelDataPair]] = []

    data_count = 0

    for timestamp_date in sorted(grouped_dict.keys()):
        data_pair = grouped_dict[timestamp_date]

        if len(data_pair) != 2:
            continue  # Pair incomplete - skip

        data_count += 1

        ret.append((timestamp_date, data_pair))

        if data_count >= count:
            # Get recent <count> data at max
            break

    return ret


def _sr_levels_range_of_pair(
    df_selected: DataFrame, *,
    group_basis: str,
) -> Generator[list[float], None, None]:
    columns = [PxDataCol.OPEN, PxDataCol.HIGH, PxDataCol.LOW]

    today = pd.Timestamp.today(tz="UTC")
    df_selected = df_selected[today - BDay(6):today]

    sr_level_data = _sr_levels_get_recent_n_only(
        df_selected.groupby(group_basis)[columns].apply(lambda df: df.to_dict("records")).to_dict(),
        5
    )

    flattened_data = [data for _, pair in sr_level_data for data in pair]

    range_high = max(flattened_data, key=lambda item: item[PxDataCol.HIGH])[PxDataCol.HIGH] * 1.02  # +2%
    range_low = min(flattened_data, key=lambda item: item[PxDataCol.LOW])[PxDataCol.LOW] * 0.98  # -2%

    for timestamp_date, data_pair in sr_level_data:
        higher = max(data_pair, key=lambda item: item[PxDataCol.OPEN])[PxDataCol.OPEN]
        lower = min(data_pair, key=lambda item: item[PxDataCol.OPEN])[PxDataCol.OPEN]

        diff = higher - lower

        if diff < SR_LEVEL_MIN_DIFF:
            yield []

        yield list(np.arange(range_low, range_high, diff))


def sr_levels_range_of_pair(df_1k: DataFrame, symbol: str) -> list[list[float]]:
    if key_timestamp := _KEY_TS_MAP.get(symbol):
        df_selected = _calc_key_time(df_1k, key_timestamp)
    else:
        raise ValueError(f"Symbol `{symbol}` does not have key time picking logic")

    levels: list[list[float]] = []

    for levels_group in _sr_levels_range_of_pair(df_selected, group_basis=PxDataCol.AUTO_SR_GROUP_BASIS):
        levels.append(levels_group)

    return levels


def sr_levels_range_of_pair_merged(df_1k: DataFrame, symbol: str) -> list[float]:
    if symbol != "FITX":
        return []

    df_selected = _calc_key_time(df_1k, _TS_FUT_TW_BASIC)

    levels: list[float] = []

    for levels_group in _sr_levels_range_of_pair(df_selected, group_basis=PxDataCol.DATE_MARKET):
        levels.extend(levels_group)

    return sorted(levels)
