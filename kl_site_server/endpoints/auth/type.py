from datetime import datetime
from typing import Literal, TypedDict


class JwtDataDict(TypedDict):
    sub: str  # JWT-compliant field - subject
    exp: datetime  # JWT-compliant field - expiry


Permission = Literal[
    "chart:view",
    "manager:add",
    "manager:remove",
    "account:new",
    "account:block",
    "account:delete",
]
