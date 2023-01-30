from unittest.mock import MagicMock

from kl_site_common.utils import DataCache


def test_data_cache_gets_correct_value():
    def get_value(key: int):
        return -key

    cache = DataCache(get_value)

    assert cache.get_value(3) == -3
    assert cache.get_value(7) == -7


def test_data_cache_gets_value_with_payload():
    def get_value(key: int, payload: int | None = None):
        return key + (payload or 0)

    cache = DataCache(get_value)

    assert cache.get_value(3) == 3
    assert cache.get_value(7, payload=3) == 10


def test_data_cache_uses_existing_value():
    fn_get_value = MagicMock()
    cache = DataCache(fn_get_value)

    cache.get_value(3)
    assert fn_get_value.call_count == 1
    cache.get_value(3)
    assert fn_get_value.call_count == 1


def test_data_cache_no_store():
    fn_get_value = MagicMock()
    cache = DataCache(fn_get_value, lambda _: True)

    cache.get_value(3)
    assert fn_get_value.call_count == 1
    cache.get_value(3)
    assert fn_get_value.call_count == 2


def test_data_cache_default_body():
    def get_value(key: int):
        return -key

    cache = DataCache(get_value, default_body={5: 5})

    assert cache.get_value(3) == -3
    assert cache.get_value(7) == -7
    assert cache.get_value(5) == 5
