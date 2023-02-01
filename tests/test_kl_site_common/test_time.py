from datetime import datetime, time, timezone

from kl_site_common.utils import time_round_second_to_min, time_hhmm_to_utc_time, time_to_total_seconds


def test_time_to_total_seconds():
    assert time_to_total_seconds(time(7, 0)) == 25200
    assert time_to_total_seconds(time(22, 30)) == 81000


def test_time_hhmm_to_utc_time():
    assert time_hhmm_to_utc_time("07:00") == time(7, 0, tzinfo=timezone.utc)
    assert time_hhmm_to_utc_time("22:30") == time(22, 30, tzinfo=timezone.utc)


def test_time_round_second_to_min():
    assert time_round_second_to_min(datetime(2022, 7, 25, 7, 0, 58)) == datetime(2022, 7, 25, 7, 1, 0)
    assert time_round_second_to_min(datetime(2022, 7, 26, 23, 59, 58)) == datetime(2022, 7, 27, 0, 0, 0)
