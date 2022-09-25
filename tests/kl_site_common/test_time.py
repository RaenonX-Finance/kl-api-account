from datetime import time, timezone

from kl_site_common.utils import time_str_to_utc_time, time_to_total_seconds


def test_time_to_total_seconds():
    assert time_to_total_seconds(time(7, 0)) == 25200
    assert time_to_total_seconds(time(22, 30)) == 81000


def test_time_str_to_utc_time():
    assert time_str_to_utc_time("07:00") == time(7, 0, tzinfo=timezone.utc)
    assert time_str_to_utc_time("22:30") == time(22, 30, tzinfo=timezone.utc)
