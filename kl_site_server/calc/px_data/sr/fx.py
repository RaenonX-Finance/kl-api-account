"""
Most of the source code originated from:
https://medium.datadriveninvestor.com/how-to-detect-support-resistance-levels-and-breakout-using-python-f8b5dac42f21.
"""
import numpy as np
from pandas import DataFrame

from kl_site_server.enums import PxDataCol


def is_far_from_level(value: float, levels: list[float], avg: float) -> bool:
    return not np.any([abs(value - level) < avg for level in levels])


def support_resistance_fractal(df: DataFrame, min_gap: float) -> list[float]:
    series_high = df[PxDataCol.HIGH].tolist()
    series_low = df[PxDataCol.LOW].tolist()

    def is_bullish_fractal(i: int):
        return (
            series_low[i] < series_low[i - 1] < series_low[i - 2]
            and series_low[i] < series_low[i + 1] < series_low[i + 2]
        )

    def is_bearish_fractal(i: int):
        return (
            series_high[i] > series_high[i - 1] < series_high[i - 2]
            and series_high[i] > series_high[i + 1] > series_high[i + 2]
        )

    levels = []

    for i in reversed(range(2, len(df.index) - 3)):
        if is_bullish_fractal(i):
            low = series_low[i]
            if is_far_from_level(low, levels, min_gap):
                levels.append(low)
        elif is_bearish_fractal(i):
            high = series_high[i]
            if is_far_from_level(high, levels, min_gap):
                levels.append(high)

    return sorted(levels)


def support_resistance_window(df: DataFrame, min_gap: float) -> list[float]:
    levels = []
    max_list = []
    min_list = []

    series_high = df[PxDataCol.HIGH].tolist()
    series_low = df[PxDataCol.LOW].tolist()

    for i in reversed(range(5, len(df.index) - 6)):
        # taking a window of 9 candles
        current_max = max(series_high[i - 5:i + 4])

        # if we find a new maximum value, empty the max_list
        if current_max not in max_list:
            max_list = []

        max_list.append(current_max)

        # if the maximum value remains the same after shifting 5 times
        if len(max_list) == 5 and is_far_from_level(current_max, levels, min_gap):
            levels.append(current_max)

        current_min = min(series_low[i - 5:i + 5])

        if current_min not in min_list:
            min_list = []

        min_list.append(current_min)

        if len(min_list) == 5 and is_far_from_level(current_min, levels, min_gap):
            levels.append(current_min)

    return sorted(levels)
