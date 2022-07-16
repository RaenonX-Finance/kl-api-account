from fastapi import APIRouter, Depends

from .db_control import (
    get_user_config as get_user_config_db, update_layout_config as update_layout_config_db,
    update_slot_map as update_slot_map_db,
)
from .model import UserConfigModel

user_router = APIRouter(prefix="/user")


@user_router.get(
    "/config/get",
    description="Get the current user config.",
    response_model=UserConfigModel,
)
async def get_user_config(config: UserConfigModel = Depends(get_user_config_db)) -> UserConfigModel:
    return config


@user_router.post(
    "/config/update-slot",
    description="Update the slot assignment.",
    response_model=dict,
)
async def update_slot_map(updated_slot_map: dict = Depends(update_slot_map_db)) -> dict:
    return updated_slot_map


@user_router.post(
    "/config/update-layout",
    description="Update the layout config of a slot.",
    response_model=dict,
)
async def update_layout_config(updated_layout_config: dict = Depends(update_layout_config_db)) -> dict:
    return updated_layout_config
