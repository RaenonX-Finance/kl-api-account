from datetime import datetime
from typing import Generator, TypeAlias

import numpy as np
from pandas import DataFrame, Series
from pandas.tseries.offsets import BDay

from kl_site_common.const import SR_LEVEL_MIN_DIFF
from kl_site_common.utils import print_warning
from kl_site_server.db import get_history_data_at_time_from_db
from kl_site_server.enums import PxDataCol
from .model import SrLevelKeyTimePair


def _get_bool_series_at_time(df: DataFrame, epoch_sec_time: int) -> Series:
    return df[PxDataCol.EPOCH_SEC_TIME] == epoch_sec_time


def _calc_key_time_add_group_basis(df: DataFrame, epoch_sec_time: int) -> DataFrame:
    df[PxDataCol.AUTO_SR_GROUP_BASIS] = np.where(
        _get_bool_series_at_time(df, epoch_sec_time),
        df[PxDataCol.DATE_MARKET] + BDay(1),
        df[PxDataCol.DATE_MARKET] + BDay(0),
    )

    return df


def _calc_key_time(df_1k: DataFrame, key_time_pair: SrLevelKeyTimePair) -> DataFrame:
    open_time_sec, close_time_sec = key_time_pair.open_time_sec, key_time_pair.close_time_sec

    day_open = _get_bool_series_at_time(df_1k, open_time_sec)
    day_close = _get_bool_series_at_time(df_1k, close_time_sec)

    return _calc_key_time_add_group_basis(df_1k[day_open | day_close].copy(), close_time_sec)


SrLevelDataPair: TypeAlias = [dict[str, float], dict[str, float]]


def _get_recent_n_sr_level_pairs_only(
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


def _generate_sr_level_pairs(
    df_selected: DataFrame, *,
    group_basis: str,
    current_px: float,
) -> Generator[list[float], None, None]:
    columns = [PxDataCol.OPEN, PxDataCol.CLOSE]

    grouped_dict = df_selected.groupby(group_basis)[columns].apply(lambda df: df.to_dict("records")).to_dict()

    sr_level_data = _get_recent_n_sr_level_pairs_only(grouped_dict, 5)

    if not sr_level_data:
        print_warning(
            f"Attempt to calculate SR data while the data pair is misformatted: {grouped_dict}",
            force=True
        )
        return

    range_high = current_px * 1.05  # +5%
    range_low = current_px * 0.95  # -5%

    for timestamp_date, data_pair in sr_level_data:
        _1, _2 = data_pair
        _1 = _1[PxDataCol.CLOSE]
        _2 = _2[PxDataCol.OPEN]

        diff = abs(_1 - _2)

        if diff < SR_LEVEL_MIN_DIFF:
            yield []
        else:
            yield list(np.concatenate([
                np.arange(min(_1, _2), range_low, -diff),
                np.arange(max(_1, _2), range_high, diff),
            ]))


def get_sr_level_pairs(
    symbol_complete: str,
    current_px: float,
    key_time_pair: SrLevelKeyTimePair
) -> list[list[float]]:
    df = DataFrame(get_history_data_at_time_from_db(
        symbol_complete,
        (key_time_pair.open_time_sec, key_time_pair.close_time_sec),
        count=12
    ).data)
    df = _calc_key_time(df, key_time_pair)

    levels: list[list[float]] = []
    levels_pair = _generate_sr_level_pairs(
        df,
        group_basis=PxDataCol.AUTO_SR_GROUP_BASIS,
        current_px=current_px
    )

    for levels_group in levels_pair:
        levels.append(levels_group)

    return levels


def get_sr_level_pairs_merged(
    symbol_complete: str,
    current_px: float,
    key_time_pair: SrLevelKeyTimePair | None,
) -> list[float]:
    if not key_time_pair:
        return []

    levels: list[float] = []
    levels_pair = get_sr_level_pairs(symbol_complete, current_px, key_time_pair)

    for levels_group in levels_pair:
        levels.extend(levels_group)

    return levels
