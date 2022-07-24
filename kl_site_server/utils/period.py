from datetime import timedelta

from kl_site_common.const import INDICATOR_EMA_PERIODS

CANDLESTICK_DIR_MACD_SLOW = 300


def get_start_offset(period_min: int) -> timedelta:
    max_period = max(*INDICATOR_EMA_PERIODS, CANDLESTICK_DIR_MACD_SLOW)

    return timedelta(minutes=period_min * max_period)
