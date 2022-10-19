import threading
from contextlib import contextmanager
from typing import ContextManager

from pymongo.client_session import ClientSession

from .const import mongo_client


_lock = threading.Lock()


@contextmanager
def start_mongo_txn() -> ContextManager[ClientSession]:
    with _lock, mongo_client.start_session(causal_consistency=True) as session, session.start_transaction():
        yield session
