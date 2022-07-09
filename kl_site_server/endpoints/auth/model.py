from datetime import datetime
from typing import Literal

from bson import ObjectId
from pydantic import BaseModel, EmailStr, Field

from kl_site_common.db import PyObjectId
from .const import DEFAULT_ACCOUNT_PERMISSIONS
from .secret import get_password_hash
from .type import Permission


class UserDataModel(BaseModel):
    """User data model. This does not and should not contain account secret, such as password."""
    id: PyObjectId | None = Field(default_factory=PyObjectId, alias="_id")
    account_id: str = Field(..., description="Account ID.")
    email: EmailStr | None = Field(None, description="User email.")
    expiry: datetime | None = Field(None, description="Account membership expiry.")
    blocked: bool = Field(False, description="If the account is blocked. Blocked account does not have access.")
    admin: bool = Field(False, description="If the account holder is an admin. Admin has all possible permissions.")
    permissions: list[Literal[Permission]] = Field(
        ...,
        description="List of permissions that the account holder has."
    )

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class DbUserModel(UserDataModel):
    """Complete user data model. This is the schema stored in the database."""
    hashed_password: str = Field(..., description="Hashed account password.")


class OAuthTokenData(BaseModel):
    """Decoded access token data from JWT."""
    account_id: str | None = None


class ActionModel(BaseModel):
    access_token: str


class OAuthToken(BaseModel):
    """OAuth2 access token and its type."""
    access_token: str
    token_type: Literal["bearer"]


class GenerateValidationSecretsModel(ActionModel):
    """Data model to generate identity validation secrets."""


class ValidationSecretsModel(BaseModel):
    """Validation secrets."""

    client_id: str
    client_secret: str


class SignupKeyModel(BaseModel):
    """Data model containing the account signup key."""
    signup_key: str = Field(..., description="Account signup key.")


class UserSignupModel(BaseModel):
    """Data model to sign up a user."""
    account_id: str = Field(..., description="Account ID.")
    password: str = Field(...)
    signup_key: str | None = Field(description="Key used to sign up this account. ")

    def to_db_user_model(self, *, admin: bool = False) -> DbUserModel:
        return DbUserModel(
            account_id=self.account_id,
            hashed_password=get_password_hash(self.password),
            admin=admin,
            permissions=DEFAULT_ACCOUNT_PERMISSIONS,
        )
