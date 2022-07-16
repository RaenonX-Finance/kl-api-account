from fastapi import Depends

from kl_site_common.db import PyObjectId
from .const import user_db_config
from .model import UpdateConfigModel, UserConfigModel
from ..auth import (
    UserDataModel, get_active_user_by_oauth2_token, get_user_data_by_oauth2_token,
)


def create_new_user_config(account_id: PyObjectId) -> UserConfigModel:
    model = UserConfigModel(
        account_id=account_id,
        slot_map={
            "FITX@1": "A",
            "NQ@1": "B",
            "YM@1": "C",
            "FITX@5": "D",
        },
        layout_config={},
        layout_type="4-2x2",
    )
    user_db_config.insert_one(model.dict())

    return model


def get_user_config(
    user: UserDataModel = Depends(get_active_user_by_oauth2_token),
) -> UserConfigModel:
    config_model = user_db_config.find_one({"account_id": user.id})

    if not config_model:
        config_model = create_new_user_config(user.id)

    return config_model


async def get_user_config_by_token(token: str) -> UserConfigModel:
    user_data = await get_user_data_by_oauth2_token(token)
    user_data = await get_active_user_by_oauth2_token(user_data)

    return get_user_config(user_data)


def update_slot_map(
    config_og: UserConfigModel = Depends(get_user_config),
    body: UpdateConfigModel = Depends()
) -> dict:
    config_model = user_db_config.find_one_and_update(
        {"account_id": config_og.account_id},
        {"$set": {"slot_map": body.data}}
    )

    return config_model.slot_map


def update_layout_config(
    config_og: UserConfigModel = Depends(get_user_config),
    body: UpdateConfigModel = Depends()
) -> dict:
    config_model = user_db_config.find_one_and_update(
        {"account_id": config_og.account_id},
        {"$set": {"layout_config": body.data}}
    )

    return config_model.layout_config
