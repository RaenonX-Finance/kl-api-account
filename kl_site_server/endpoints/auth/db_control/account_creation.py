import secrets

from fastapi import Depends

from .auth_user import get_admin_user_by_oauth2_token
from ..const import auth_db_signup_key
from ..model import SignupKeyModel, UserDataModel


async def generate_account_creation_key(_: UserDataModel = Depends(get_admin_user_by_oauth2_token)) -> SignupKeyModel:
    model = SignupKeyModel(signup_key=secrets.token_hex(32))
    auth_db_signup_key.insert_one(model.dict())

    return model
