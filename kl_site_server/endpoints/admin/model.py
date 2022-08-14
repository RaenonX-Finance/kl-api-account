from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, root_validator

from kl_site_common.db import PyObjectId
from kl_site_server.db import Permission


class AccountData(BaseModel):
    id: str
    username: str
    permissions: list[Permission]
    expiry: Optional[datetime]
    blocked: bool
    admin: bool
    online: bool


class ExpiryUpdateModel(BaseModel):
    id: PyObjectId = Field(...)
    expiry: datetime | None = Field(...)


class BlockedUpdateModel(BaseModel):
    id: PyObjectId = Field(...)
    blocked: bool = Field(...)


class PermissionUpdateModel(BaseModel):
    id: PyObjectId = Field(...)
    add: list[Permission] = Field(...)
    remove: list[Permission] = Field(...)

    @root_validator
    def check_permission_to_change(cls, values: dict[str, Any]) -> dict[str, Any]:
        permissions_add = values.get("add")
        permissions_remove = values.get("remove")

        if permissions_add is None or permissions_remove is None:
            return values  # Early termination - validation fails but let `pydantic` handle it

        if not permissions_add and not permissions_remove:
            raise ValueError("Either `add` or `remove` should have length > 0")

        return values
