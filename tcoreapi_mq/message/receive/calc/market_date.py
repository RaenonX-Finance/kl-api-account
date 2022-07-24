from datetime import datetime, timezone
from typing import Callable

from pandas.tseries.offsets import BDay

from kl_site_common.utils import get_epoch_sec_time


def _calc_market_date_nq_ym(timestamp: datetime, epoch_sec_time: float) -> datetime:
    return timestamp.date() + BDay(0 if epoch_sec_time < get_epoch_sec_time(22) else 1)


def _calc_market_date_fitx(timestamp: datetime, epoch_sec_time: float) -> datetime:
    return timestamp.date() + BDay(0 if epoch_sec_time >= get_epoch_sec_time(0, 45) else 1)


_calc_function_map: dict[str, Callable[[datetime, float], datetime]] = {
    "TC.F.CBOT.YM.HOT": _calc_market_date_nq_ym,
    "TC.F.CME.NQ.HOT": _calc_market_date_nq_ym,
    "TC.F.TWF.FITX.HOT": _calc_market_date_fitx,
}


def calc_market_date(timestamp: datetime, epoch_sec_time: float, symbol_complete: str) -> datetime:
    if calc_market_date_symbol := _calc_function_map.get(symbol_complete):
        return calc_market_date_symbol(timestamp, epoch_sec_time).replace(tzinfo=timezone.utc)

    raise ValueError(f"Symbol `{symbol_complete}` does not have market date calculation logic")
