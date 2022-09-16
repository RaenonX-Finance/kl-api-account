from datetime import timedelta

from kl_site_common.const import INDICATOR_EMA_PERIODS

CANDLESTICK_DIR_MACD_SLOW = 300

MAX_PERIOD = max(*INDICATOR_EMA_PERIODS, CANDLESTICK_DIR_MACD_SLOW)

MAX_PERIOD_NO_EMA = CANDLESTICK_DIR_MACD_SLOW


def get_start_offset(period_min: int) -> timedelta:
    return timedelta(minutes=period_min * MAX_PERIOD)
