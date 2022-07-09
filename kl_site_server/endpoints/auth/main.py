from fastapi import APIRouter, Depends, status

from .db_control import (
    generate_access_token, generate_access_token_on_doc, generate_validation_secrets,
    get_active_user_by_oauth2_token, signup_user, generate_account_creation_key as generate_account_creation_key_db
)
from .model import OAuthToken, UserDataModel, ValidationSecretsModel, SignupKeyModel

auth_router = APIRouter(prefix="/auth")


@auth_router.get(
    "/me",
    description="Get the user data using the access token.",
    response_model=UserDataModel,
)
async def get_user_data(current_user: UserDataModel = Depends(get_active_user_by_oauth2_token)) -> UserDataModel:
    return current_user


@auth_router.post(
    "/token",
    description="Get an access token using account credentials.",
    response_model=OAuthToken
)
async def get_access_token_by_credentials(access_token: str = Depends(generate_access_token)) -> OAuthToken:
    return OAuthToken(access_token=access_token, token_type="bearer")


@auth_router.post(
    "/token-doc",
    description="Get an access token using account credentials. Should be used on the interactive document only.",
    response_model=OAuthToken
)
async def get_access_token_by_credentials_on_doc(
    access_token: str = Depends(generate_access_token_on_doc)
) -> OAuthToken:
    return OAuthToken(access_token=access_token, token_type="bearer")


@auth_router.post(
    "/validation-secrets",
    description="Create secrets to use for validation. "
                "Only admin can perform this action. "
                "Replaces the existing secrets.",
    response_model=ValidationSecretsModel,
    status_code=status.HTTP_201_CREATED
)
async def create_validation_secrets(
    secrets: ValidationSecretsModel = Depends(generate_validation_secrets)
) -> ValidationSecretsModel:
    return secrets


@auth_router.post(
    "/signup",
    description="Signup a user.",
    response_model=UserDataModel,
    status_code=status.HTTP_201_CREATED
)
async def sign_up_user(user: UserDataModel = Depends(signup_user)) -> UserDataModel:
    return user


@auth_router.post(
    "/generate-account",
    description="Generate an account signup key.",
    response_model=str,
    status_code=status.HTTP_201_CREATED
)
async def generate_account_creation_key(
    signup_key: SignupKeyModel = Depends(generate_account_creation_key_db)
) -> str:
    return signup_key.signup_key
