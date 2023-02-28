from datetime import datetime

from pydantic import BaseModel, Field

from kl_api_common.db import PyObjectId


class UserSessionModel(BaseModel):
    account_id: PyObjectId = Field(..., description="Account ID (ObjectID) of the session.")
    session_id: str = Field(..., description="Session ID of the connecting `socket.io` client.")
    last_check: datetime = Field(..., description="Last session check timestamp.")
