import json
from typing import TypedDict

from tcoreapi_mq.message import SystemTimeData


class ServerMinChangeMessage(TypedDict):
    epochSec: int


def to_socket_min_change(data: SystemTimeData) -> str:
    message: ServerMinChangeMessage = {"epochSec": int(data.epoch_sec)}

    return json.dumps(message)
