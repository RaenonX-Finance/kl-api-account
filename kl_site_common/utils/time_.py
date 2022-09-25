from datetime import datetime, time, timezone


def time_str_to_utc_time(s: str) -> time:
    return datetime.strptime(s, "%H:%M").time().replace(tzinfo=timezone.utc)


def time_to_total_seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second
