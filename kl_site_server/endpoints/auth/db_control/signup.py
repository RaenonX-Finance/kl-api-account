import pymongo.errors
from fastapi import Depends

from .auth_user import get_user_data_by_username
from ..const import auth_db_signup_key, auth_db_users
from ..exceptions import generate_bad_request_exception
from ..model import UserDataModel, UserSignupModel


async def signup_user_ensure_unique(user: UserSignupModel = Depends()) -> UserSignupModel:
    if auth_db_users.count_documents({}) == 0:
        # No user exists - user to signup is the admin
        auth_db_users.insert_one(user.to_db_user_model(admin=True).dict())

        return user

    if not auth_db_signup_key.find_one_and_delete({"signup_key": user.signup_key}):
        raise generate_bad_request_exception("Invalid signup key")

    try:
        auth_db_users.insert_one(user.to_db_user_model(admin=False).dict())
    except pymongo.errors.DuplicateKeyError as ex:
        raise generate_bad_request_exception("Duplicated account ID") from ex

    return user


async def signup_user(user: UserSignupModel = Depends(signup_user_ensure_unique)) -> UserDataModel:
    return await get_user_data_by_username(user.username)
