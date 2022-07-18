import pymongo
from pymongo.collection import Collection
from pymongo.database import Database

from kl_site_common.db import mongo_client

px_db: Database = mongo_client.get_database("px")

px_data_col: Collection = Collection(px_db, "data")
px_data_col.create_index([
    ("ts", pymongo.ASCENDING),
    ("s", pymongo.ASCENDING),
    ("i", pymongo.ASCENDING)
], unique=True)
