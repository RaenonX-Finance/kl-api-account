from typing import TypedDict

from kl_api_account.db import UserConfigModel


class InitData(TypedDict):
    config: dict


def to_socket_message_init_data(config: "UserConfigModel") -> InitData:
    data: InitData = {
        # `account_id` field is not needed
        # `account_id` also has JSON serialization issue caused by socket IO server
        "config": config.dict(),
    }

    return data
