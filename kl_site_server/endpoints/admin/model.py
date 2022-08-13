from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

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
