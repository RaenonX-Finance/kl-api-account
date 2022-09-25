from datetime import time


def time_to_total_seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second
