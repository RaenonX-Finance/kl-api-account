from datetime import datetime

from bson import ObjectId
from pydantic import BaseModel, EmailStr, Field

from kl_site_common.db import PyObjectId
from .type import Permission


class UserDataModel(BaseModel):
    """User data model. This does not and should not contain account secret, such as password."""
    id: PyObjectId | None = Field(default_factory=PyObjectId, alias="_id")
    username: str = Field(..., description="User name.")
    email: EmailStr | None = Field(None, description="User email.")
    expiry: datetime | None = Field(None, description="Account membership expiry.")
    blocked: bool = Field(False, description="If the account is blocked. Blocked account does not have access.")
    admin: bool = Field(False, description="If the account holder is an admin. Admin has all possible permissions.")
    permissions: list[Permission] = Field(
        ...,
        description="List of permissions that the account holder has."
    )

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

    def has_permission(self, permission: Permission) -> bool:
        if self.admin:
            return True

        return permission in self.permissions


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


class ValidationSecretsModel(BaseModel):
    """Validation secrets."""
    client_id: str
    client_secret: str


class SignupKeyGenerationModel(BaseModel):
    """Data model used when generating an account."""
    account_expiry: datetime = Field(
        ...,
        description="Account expiry. "
                    "User who signup with this key will have this time as the initial account membership expiry."
    )


class SignupKeyModel(SignupKeyGenerationModel):
    """
    Data model containing the account signup key.

    This model is used in the database.
    """
    signup_key: str = Field(..., description="Account signup key.")
    expiry: datetime = Field(..., description="Signup key expiry. The signup key will be deleted after this time.")
