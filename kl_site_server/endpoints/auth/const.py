from typing import TYPE_CHECKING

from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from pymongo.collection import Collection
from pymongo.database import Database

from kl_site_common.db import mongo_client
from .type import Permission

if TYPE_CHECKING:
    from .model import DbUserModel, ValidationSecretsModel

auth_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")
auth_crypto_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

auth_db: Database = mongo_client.get_database("auth")

auth_db_users: Collection["DbUserModel"] = Collection(auth_db, "users")
auth_db_users.create_index("account_id", unique=True)

auth_db_validation: Collection["ValidationSecretsModel"] = auth_db.get_collection("validation")
if auth_db_validation is None:
    auth_db_validation = auth_db.create_collection("validation", capped=True, size=4096, max=1)

DEFAULT_ACCOUNT_PERMISSIONS: list[Permission] = []
