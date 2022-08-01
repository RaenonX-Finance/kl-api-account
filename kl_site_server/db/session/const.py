from typing import TYPE_CHECKING

from pymongo.collection import Collection
from pymongo.database import Database

from kl_site_common.db import mongo_client

if TYPE_CHECKING:
    from .model import UserSessionModel


user_db: Database = mongo_client.get_database("user")

user_db_session: Collection["UserSessionModel"] = Collection(user_db, "session")
user_db_session.create_index("account_id", unique=True)
