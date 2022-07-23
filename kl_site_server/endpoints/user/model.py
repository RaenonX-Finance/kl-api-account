from typing import Literal, TypeAlias, Union

from bson import ObjectId
from pydantic import BaseModel, Field

from kl_site_common.db import PyObjectId

PxSlotName: TypeAlias = Literal["A", "B", "C", "D"]

LayoutType: TypeAlias = Literal[
    "1-1x1",
    "2-2x1",
    "2-1x2",
    "3-3x1",
    "3-1x3",
    "3-LF",
    "3-RF",
    "3-TF",
    "3-BF",
    "4-2x2",
    "4-4x1",
    "4-1x4",
    "4-LF",
    "4-RF",
    "4-TF",
    "4-BF",
    "4-L2",
    "4-R2",
    "4-T2",
    "4-B2"
]

ConfigKeyStringData: TypeAlias = Literal[
    "layout_type",
]

ConfigKeyDictData: TypeAlias = Literal[
    "slot_map",
    "layout_config",
    "shared_config",
]


class UpdateConfigModelStringData(BaseModel):
    """Config update data model with data being :class:`str`."""
    key: ConfigKeyStringData = Field(..., description="Key of the config to update.")
    data: str = Field(..., description="Updated config dict.")


class UpdateConfigModelDictData(BaseModel):
    """Config update data model with data being :class:`dict`."""
    key: ConfigKeyDictData = Field(..., description="Key of the config to update.")
    data: dict = Field(..., description="Updated config dict.")


UpdateConfigModel: TypeAlias = Union[UpdateConfigModelStringData, UpdateConfigModelDictData]


class UserConfigModel(BaseModel):
    """
    Data model containing user config.

    This model is used in the database.

    ``None`` values mean to use the default config for the frontend,
    and the corresponding config has not been backed up yet.
    """
    account_id: PyObjectId = Field(..., description="Account ID (ObjectID) who has this config.")
    slot_map: dict[PxSlotName, str] | None = Field(
        None,
        description="Slot mapping to Px data identifier. "
                    "Px data identifier should be in the format of `<Security>@<PeriodMin>`."
    )
    layout_type: LayoutType | None = Field(None, description="Layout type.")
    layout_config: dict | None = Field(None, description="Layout config of slots.")
    shared_config: dict | None = Field(None, description="Shared config for Px cahrts.")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
