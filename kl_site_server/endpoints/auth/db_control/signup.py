import pymongo.errors
from fastapi import Depends

from .auth_user import get_user_data_by_account_id
from ..const import auth_db_users
from ..exceptions import generate_bad_request_exception
from ..model import UserDataModel, UserSignupModel


async def signup_user_ensure_unique(user: UserSignupModel = Depends()) -> UserSignupModel:
    if auth_db_users.count_documents({}) == 0:
        auth_db_users.insert_one(user.to_db_user_model(admin=True).dict())

        return user

    try:
        auth_db_users.insert_one(user.to_db_user_model(admin=False).dict())
    except pymongo.errors.DuplicateKeyError as ex:
        raise generate_bad_request_exception("Duplicated account ID") from ex

    return user


async def signup_user(user: UserSignupModel = Depends(signup_user_ensure_unique)) -> UserDataModel:
    return await get_user_data_by_account_id(user.account_id)
