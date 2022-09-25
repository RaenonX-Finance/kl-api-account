from datetime import time

from kl_site_common.utils import time_to_total_seconds


def test_time_to_total_seconds():
    assert time_to_total_seconds(time(7, 0)) == 25200
    assert time_to_total_seconds(time(22, 30)) == 81000
