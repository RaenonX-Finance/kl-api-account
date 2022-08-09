from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from kl_site_server.db import DEFAULT_ACCOUNT_PERMISSIONS, DbUserModel
from .secret import get_password_hash


class OAuthToken(BaseModel):
    """OAuth2 access token and its type."""
    access_token: str
    token_type: Literal["bearer"]


class RefreshAccessTokenModel(BaseModel):
    """Data model containing the data needed for refreshing the access token."""
    client_id: str = Field(..., description="OAuth client ID.")
    client_secret: str = Field(..., description="OAuth client secret.")


class UserSignupModel(BaseModel):
    """Data model to sign up a user."""
    username: str = Field(..., min_length=6)
    password: str = Field(..., min_length=8)
    signup_key: str | None = Field(None, description="Key used to sign up this account.")

    def to_db_user_model(self, *, expiry: datetime | None, admin: bool = False) -> DbUserModel:
        if not admin and not self.signup_key:
            raise ValueError("The user is not an admin, but `signup_key` is `None`.")

        return DbUserModel(
            # From `UserDataModel`
            username=self.username,
            admin=admin,
            expiry=expiry,
            permissions=DEFAULT_ACCOUNT_PERMISSIONS,
            # From `DbUserModel`
            hashed_password=get_password_hash(self.password),
            signup_key=None if admin else self.signup_key,
        )
