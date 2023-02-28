import json
from typing import TypedDict

from kl_api_account.db import UserConfigModel
from kl_api_common.utils import JSONEncoder


class InitData(TypedDict):
    config: dict


def to_socket_message_init_data(config: "UserConfigModel") -> str:
    data: InitData = {
        "config": dict(config),
    }

    return json.dumps(data, cls=JSONEncoder)
