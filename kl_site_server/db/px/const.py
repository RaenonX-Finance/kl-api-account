from typing import TYPE_CHECKING
import pymongo
from pymongo.collection import Collection
from pymongo.database import Database

from kl_site_common.db import mongo_client

if TYPE_CHECKING:
    from .model import FuturesMarketClosedSessionModel

px_db: Database = mongo_client.get_database("px")

px_data_col: Collection = Collection(px_db, "data")
px_data_col.create_index([
    ("ts", pymongo.ASCENDING),
    ("s", pymongo.ASCENDING),
    ("i", pymongo.ASCENDING)
], unique=True)

px_session_col: Collection["FuturesMarketClosedSessionModel"] = Collection(px_db, "session")
px_session_col.create_index("end", expireAfterSeconds=0)
px_session_col.create_index([
    ("security", pymongo.ASCENDING),
    ("start", pymongo.ASCENDING),
    ("end", pymongo.ASCENDING)
], unique=True)
