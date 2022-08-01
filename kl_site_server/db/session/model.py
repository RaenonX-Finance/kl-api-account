from pydantic import BaseModel, Field

from kl_site_common.db import PyObjectId
from kl_site_server.utils import SocketNamespace


class UserSessionModel(BaseModel):
    account_id: PyObjectId = Field(..., description="Account ID (ObjectID) of the session.")
    session_id: dict[SocketNamespace, str] = Field(
        ...,
        description="Session ID of the `socket.io` client, "
                    "where key is the socket namespace; value is the session ID."
    )
