from typing import Callable, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class DataCache:
    def __init__(self, fn_get_value: Callable[[K], V]):
        self._body: dict[K, V] = {}
        self._fn_get_value = fn_get_value

    def get_value(self, key: K) -> V:
        if value := self._body.get(key):
            return value

        value = self._fn_get_value(key)
        self._body[key] = value

        return value
