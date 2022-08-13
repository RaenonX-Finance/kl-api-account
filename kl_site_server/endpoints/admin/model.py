from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from kl_site_server.db import Permission


class AccountData(BaseModel):
    username: str
    permissions: list[Permission]
    expiry: Optional[datetime]
    blocked: bool
    admin: bool
    online: bool
