from typing import TypeVar

from fastapi import APIRouter, Depends

from .db_control import update_config as update_config_db

user_router = APIRouter(prefix="/user")

T = TypeVar("T")


@user_router.post(
    "/config/update",
    description="Update config.",
    response_model=T,
)
async def update_config(updated_slot_map: T = Depends(update_config_db)) -> T:
    return updated_slot_map
