import json
from typing import TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from kl_site_server.model import OnErrorEvent


class ErrorMessage(TypedDict):
    message: str


def to_socket_message_error(e: "OnErrorEvent") -> str:
    data: ErrorMessage = {
        "message": e.message,
    }

    return json.dumps(data)
