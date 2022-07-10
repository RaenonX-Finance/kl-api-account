import secrets
from datetime import datetime, timedelta, timezone

from fastapi import Depends, Form

from kl_site_common.const import ACCOUNT_SIGNUP_KEY_EXPIRY_SEC
from .auth_user import get_admin_user_by_oauth2_token
from ..const import auth_db_signup_key
from ..model import SignupKeyModel, UserDataModel


async def generate_account_creation_key(
    _: UserDataModel = Depends(get_admin_user_by_oauth2_token),
    expiry: datetime = Form(...),
) -> SignupKeyModel:
    model = SignupKeyModel(
        signup_key=secrets.token_hex(32),
        expiry=datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(seconds=ACCOUNT_SIGNUP_KEY_EXPIRY_SEC),
        account_expiry=expiry.replace(tzinfo=timezone.utc),
    )
    auth_db_signup_key.insert_one(model.dict())

    return model
