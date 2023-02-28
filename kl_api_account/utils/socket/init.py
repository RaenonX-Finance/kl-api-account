import json
from typing import TypedDict

from kl_api_account.db import UserConfigModel


class InitData(TypedDict):
    config: "UserConfigModel"


def to_socket_message_init_data(config: "UserConfigModel") -> str:
    data: InitData = {
        "config": config,
    }

    return json.dumps(data)
