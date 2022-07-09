"""
Use ``pydantic`` models for database or response model.

Use ``dataclass`` with ``fastapi.Form()`` for form data and intermediate data models.

> Content type of ``POST`` request should use ``dataclass`` or normal class
> to correctly parse the data sent via ``Content-Type`` of ``application/x-www-form-urlencoded``.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from bson import ObjectId
from fastapi import Form
from pydantic import BaseModel, EmailStr, Field

from kl_site_common.db import PyObjectId
from .const import DEFAULT_ACCOUNT_PERMISSIONS
from .secret import get_password_hash
from .type import Permission


class UserDataModel(BaseModel):
    """User data model. This does not and should not contain account secret, such as password."""
    id: PyObjectId | None = Field(default_factory=PyObjectId, alias="_id")
    username: str = Field(..., description="User name.")
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
    """
    Complete user data model.

    This model is used in the database.
    """
    hashed_password: str = Field(..., description="Hashed account password.")
    signup_key: str | None = Field(
        ...,
        description="Key used when the user signed up. `None` if the user is the admin."
    )


class ActionModel(BaseModel):
    access_token: str


class OAuthToken(BaseModel):
    """OAuth2 access token and its type."""
    access_token: str
    token_type: Literal["bearer"]


class ValidationSecretsModel(BaseModel):
    """Validation secrets."""

    client_id: str
    client_secret: str


class SignupKeyModel(BaseModel):
    """
    Data model containing the account signup key.

    This model is used in the database.
    """
    signup_key: str = Field(..., description="Account signup key.")


@dataclass
class UserSignupModel:
    """Data model to sign up a user."""

    username: str = Form(...)
    password: str = Form(...)
    signup_key: str | None = Form(None, description="Key used to sign up this account. ")

    def to_db_user_model(self, *, admin: bool = False) -> DbUserModel:
        if not admin and not self.signup_key:
            raise ValueError("The user is not an admin, but `signup_key` is `None`.")

        return DbUserModel(
            username=self.username,
            hashed_password=get_password_hash(self.password),
            admin=admin,
            permissions=DEFAULT_ACCOUNT_PERMISSIONS,
            signup_key=None if admin else self.signup_key,
        )
