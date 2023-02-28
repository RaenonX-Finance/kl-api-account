from typing import TYPE_CHECKING

from pymongo.collection import Collection
from pymongo.database import Database

from kl_api_common.db import mongo_client

if TYPE_CHECKING:
    from .model import UserConfigModel

user_db: Database = mongo_client.get_database("user")

user_db_config: Collection["UserConfigModel"] = Collection(user_db, "config")
user_db_config.create_index("account_id", unique=True)
