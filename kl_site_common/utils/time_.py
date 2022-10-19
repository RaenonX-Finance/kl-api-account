from datetime import datetime, time, timedelta, timezone


def time_str_to_utc_time(s: str) -> time:
    return datetime.strptime(s, "%H:%M").time().replace(tzinfo=timezone.utc)


def time_to_total_seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def time_round_second_to_min(dt: datetime) -> datetime:
    if dt.second > 30:
        dt += timedelta(minutes=1)
        dt = dt.replace(second=0)

    return dt
