from contextlib import contextmanager

from .const import mongo_client


@contextmanager
def start_mongo_transaction():
    with mongo_client.start_session(causal_consistency=True) as session:
        with session.start_transaction():
            yield session
