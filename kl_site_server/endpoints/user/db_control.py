from typing import Any

from fastapi import Body, Depends
from pymongo import ReturnDocument

from kl_site_common.db import PyObjectId
from .const import user_db_config
from .model import UpdateConfigModel, UserConfigModel
from ..auth import UserDataModel, get_active_user_by_user_data, get_active_user_by_oauth2_token


def create_new_user_config(account_id: PyObjectId) -> UserConfigModel:
    model = UserConfigModel(
        account_id=account_id,
        slot_map=None,
        layout_type=None,
        layout_config=None,
        shared_config=None,
    )
    user_db_config.insert_one(model.dict())

    return model


def get_user_config(
    user: UserDataModel = Depends(get_active_user_by_user_data),
) -> UserConfigModel:
    config_model = user_db_config.find_one({"account_id": user.id})

    if not config_model:
        return create_new_user_config(user.id)

    return UserConfigModel(**config_model)


def get_user_config_by_token(token: str) -> UserConfigModel:
    user_data = get_active_user_by_oauth2_token(token)

    return get_user_config(user_data)


def update_config(
    config_og: UserConfigModel = Depends(get_user_config),
    body: UpdateConfigModel = Body(..., discriminator="key")
) -> Any:
    config_model = user_db_config.find_one_and_update(
        {"account_id": config_og.account_id},
        {"$set": {body.key: body.data}},
        return_document=ReturnDocument.AFTER
    )

    return config_model[body.key]
