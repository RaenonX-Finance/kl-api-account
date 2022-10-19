from threading import Lock
from typing import Callable, Protocol, TypeVar

K = TypeVar("K")
V = TypeVar("V")
P = TypeVar("P")

_no_payload = object()


class FuncGetValue(Protocol):
    def __call__(self, key: K, payload: P | None = None) -> V:
        ...


class DataCache:
    def __init__(self, fn_get_value: FuncGetValue, fn_no_store: Callable[[V], bool] | None = None):
        self._body: dict[K, V] = {}
        self._fn_get_value = fn_get_value
        self._fn_no_store = fn_no_store or (lambda _: False)
        self._lock: Lock = Lock()

    def get_value(self, key: K, *, payload: P = _no_payload) -> V:
        with self._lock:
            if value := self._body.get(key):
                return value

            if payload is not _no_payload:
                value = self._fn_get_value(key, payload)
            else:
                value = self._fn_get_value(key)

            # Store the value only if it's not `None`
            if not self._fn_no_store(value):
                self._body[key] = value

            return value
