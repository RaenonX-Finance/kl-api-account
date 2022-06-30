from typing import Callable

import pandas as pd
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def _calc_key_time_nq_ym(df_1k: DataFrame):
    day_open = df_1k[PxDataCol.DATE].dt.time == pd.to_datetime("20:00").time()
    day_close = df_1k[PxDataCol.DATE].dt.time == pd.to_datetime("13:30").time()

    return df_1k[day_open | day_close]


def _calc_key_time_fitx_1(df_1k: DataFrame) -> DataFrame:
    day_open = df_1k[PxDataCol.DATE].dt.time == pd.to_datetime("00:45").time()
    day_close = df_1k[PxDataCol.DATE].dt.time == pd.to_datetime("05:30").time()

    return df_1k[day_open | day_close]


_calc_function_map: dict[str, Callable[[DataFrame], DataFrame]] = {
    "NQ": _calc_key_time_nq_ym,
    "YM": _calc_key_time_nq_ym,
    "FITX": _calc_key_time_fitx_1,
}


def support_resistance_range_of_2_close(df_1k: DataFrame, symbol: str) -> list[list[float]]:
    if calc_key_time := _calc_function_map.get(symbol):
        df_selected = calc_key_time(df_1k)
    else:
        raise ValueError(f"Symbol `{symbol}` does not have key time picking logic")

    values_dict = df_selected.groupby(PxDataCol.DATE_MARKET)[PxDataCol.OPEN].apply(list).to_dict()
    levels: list[list[float]] = []

    for level_pair in values_dict.values():
        if len(level_pair) != 2:
            continue  # Pair incomplete - skip

        higher = max(level_pair)
        lower = min(level_pair)

        diff = higher - lower

        levels_group = []
        levels_group.extend([lower - diff * diff_mult for diff_mult in range(8)])
        levels_group.extend([higher + diff * diff_mult for diff_mult in range(8)])

        levels.append(sorted(levels_group))

    return levels
