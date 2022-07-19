from contextlib import contextmanager
from typing import ContextManager

from pymongo.client_session import ClientSession

from .const import mongo_client


@contextmanager
def start_mongo_txn() -> ContextManager[ClientSession]:
    with mongo_client.start_session(causal_consistency=True) as session:
        with session.start_transaction():
            yield session
