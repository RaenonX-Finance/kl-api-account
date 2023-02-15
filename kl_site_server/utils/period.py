import math
from datetime import datetime

from pandas.tseries.offsets import BDay

from kl_site_server.db import PX_CONFIG

_PERIODS_NO_EMA = [
    PX_CONFIG.candle_dir.signal + PX_CONFIG.candle_dir.slow,
    PX_CONFIG.candle_dir.fast
]

MAX_PERIOD = max(*PX_CONFIG.ema_periods, *PX_CONFIG.candle_dir.ema_periods)

MAX_PERIOD_NO_EMA = max(*_PERIODS_NO_EMA)


def get_dt_before_offset(base: datetime, period_min: int) -> datetime:
    return (base - BDay(math.ceil(period_min * MAX_PERIOD / 1440))).to_pydatetime()
