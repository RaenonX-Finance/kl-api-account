import math
from datetime import datetime

from pandas.tseries.offsets import BDay

from kl_site_server.db import PX_CONFIG


MAX_PERIOD = max(*PX_CONFIG.ema_periods, *PX_CONFIG.candle_dir.ema_periods)


def get_dt_before_offset(base: datetime, period_min: int) -> datetime:
    return (base - BDay(math.ceil(period_min * MAX_PERIOD / 1440))).to_pydatetime()
