from typing import TYPE_CHECKING
from datetime import timedelta

if TYPE_CHECKING:
    from tcoreapi_mq.message import HistoryInterval


def calc_interval_to_timedelta_offset(interval: "HistoryInterval") -> timedelta:
    match interval:
        case "1K":
            return timedelta(minutes=1)
        case "DK":
            return timedelta(days=1)
        case _:
            raise ValueError(f"Unable to get `timedelta` from interval `{interval}`")
