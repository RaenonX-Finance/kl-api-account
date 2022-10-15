import math
from datetime import datetime

from pandas.tseries.offsets import BDay

from kl_site_common.const import INDICATOR_EMA_PERIODS

CANDLESTICK_DIR_MACD_FAST = 20

CANDLESTICK_DIR_MACD_SIGNAL = 15

CANDLESTICK_DIR_MACD_SLOW = 300

CANDLESTICK_DIR_MACD_REQUIRED = CANDLESTICK_DIR_MACD_SIGNAL + CANDLESTICK_DIR_MACD_SLOW

_PERIODS_NO_EMA = [CANDLESTICK_DIR_MACD_REQUIRED, CANDLESTICK_DIR_MACD_FAST]

MAX_PERIOD = max(*INDICATOR_EMA_PERIODS, *_PERIODS_NO_EMA)

MAX_PERIOD_NO_EMA = max(*_PERIODS_NO_EMA)


def get_dt_before_offset(base: datetime, period_min: int) -> datetime:
    return (base - BDay(math.ceil(period_min * MAX_PERIOD / 1440))).to_pydatetime()
