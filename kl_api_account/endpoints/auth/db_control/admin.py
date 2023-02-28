import secrets

from fastapi import Depends

from kl_api_account.db import UserDataModel, ValidationSecretsModel, auth_db_validation
from .auth_user import get_admin_user_by_oauth2_token
from ..const import auth_crypto_ctx


def generate_validation_secrets(
    _: UserDataModel = Depends(get_admin_user_by_oauth2_token)
) -> ValidationSecretsModel:
    model = ValidationSecretsModel(
        client_id=auth_crypto_ctx.hash(secrets.token_hex(32)),
        client_secret=auth_crypto_ctx.hash(secrets.token_urlsafe(32)),
    )
    auth_db_validation.insert_one(model.dict())

    return model
