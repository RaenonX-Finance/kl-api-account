from typing import Literal, TypeAlias, Union

from pydantic import BaseModel, Field

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
