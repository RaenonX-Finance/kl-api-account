from typing import Callable, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class DataCache:
    def __init__(self, fn_get_value: Callable[[K], V], fn_no_store: Callable[[V], bool] | None = None):
        self._body: dict[K, V] = {}
        self._fn_get_value = fn_get_value
        self._fn_no_store = fn_no_store or (lambda _: False)

    def get_value(self, key: K) -> V:
        if value := self._body.get(key):
            return value

        value = self._fn_get_value(key)

        # Store the value only if it's not `None`
        if not self._fn_no_store(value):
            self._body[key] = value

        return value
