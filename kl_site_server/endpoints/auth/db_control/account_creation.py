import secrets
from datetime import datetime, timedelta, timezone

from fastapi import Body, Depends

from kl_site_common.const import ACCOUNT_SIGNUP_KEY_EXPIRY_SEC
from .auth_user import get_admin_user_by_oauth2_token
from ..const import auth_db_signup_key
from ..model import SignupKeyModel, UserDataModel


async def generate_account_creation_key(
    _: UserDataModel = Depends(get_admin_user_by_oauth2_token),
    expiry: datetime = Body(...),
) -> SignupKeyModel:
    model = SignupKeyModel(
        signup_key=secrets.token_hex(32),
        expiry=datetime.utcnow().astimezone(timezone.utc) + timedelta(seconds=ACCOUNT_SIGNUP_KEY_EXPIRY_SEC),
        account_expiry=expiry.astimezone(timezone.utc),
    )
    auth_db_signup_key.insert_one(model.dict())

    return model
