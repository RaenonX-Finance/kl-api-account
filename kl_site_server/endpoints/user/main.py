from fastapi import APIRouter, Depends

from .db_control import (
    update_layout_config as update_layout_config_db,
    update_slot_map as update_slot_map_db,
)

user_router = APIRouter(prefix="/user")


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
