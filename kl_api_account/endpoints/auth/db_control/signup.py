import pymongo.errors
from fastapi import Body, Depends

from kl_api_common.db import start_mongo_txn
from kl_api_account.db import auth_db_signup_key, auth_db_users, SignupKeyModel, UserDataModel
from kl_api_account.utils import generate_bad_request_exception
from .auth_user import get_user_data_by_username
from ..model import UserSignupModel


def signup_user_ensure_unique(user: UserSignupModel = Body(...)) -> UserSignupModel:
    if auth_db_users.count_documents({}) == 0:
        # No user exists - user to signup is the admin
        auth_db_users.insert_one(user.to_db_user_model(admin=True, expiry=None).dict())

        return user

    with start_mongo_txn() as session:
        signup_key_entry = auth_db_signup_key.find_one_and_delete({"signup_key": user.signup_key}, session=session)
        if not signup_key_entry:
            raise generate_bad_request_exception("Invalid signup key")

        signup_key_entry = SignupKeyModel(**signup_key_entry)

        try:
            auth_db_users.insert_one(
                user.to_db_user_model(admin=False, expiry=signup_key_entry.account_expiry).dict(),
                session=session
            )
        except pymongo.errors.DuplicateKeyError as ex:
            raise generate_bad_request_exception("Duplicated account ID") from ex

    return user


def signup_user(user: UserSignupModel = Depends(signup_user_ensure_unique)) -> UserDataModel:
    return get_user_data_by_username(user.username)
