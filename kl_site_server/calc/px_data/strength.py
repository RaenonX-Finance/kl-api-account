import numpy as np


CALC_STRENGTH_BARS_NEEDED = 70


def _calc_strength_single(close_px: list[float], short_period: int, long_period: int) -> int:
    last_close = close_px[-1]
    avg_short = np.mean(close_px[-short_period:])
    avg_long = np.mean(close_px[-long_period:])

    if last_close > avg_short > avg_long:
        return 1
    elif last_close < avg_short < avg_long:
        return -1

    return 0


def calc_strength(close_px: list[float]) -> int:
    return sum(
        _calc_strength_single(close_px, 5 * k_period, 10 * k_period)
        for k_period in [1, 3, 5]
    )
