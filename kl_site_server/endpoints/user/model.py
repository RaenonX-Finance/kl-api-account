"""
Use ``pydantic`` models for database or response model.

Use ``dataclass`` with ``fastapi.Form()`` for form data and intermediate data models.

> Content type of ``POST`` request should use ``dataclass`` or normal class
> to correctly parse the data sent via ``Content-Type`` of ``application/x-www-form-urlencoded``.
"""
from dataclasses import dataclass
from typing import Literal, TypeAlias

from bson import ObjectId
from fastapi import Form
from pydantic import BaseModel, Field

from kl_site_common.db import PyObjectId

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


@dataclass
class UpdateConfigModel:
    """Config update data model."""
    data: dict = Form(..., description="Config object. The placement of the object depends on the called EP.")


class UserConfigModel(BaseModel):
    """
    Data model containing user config.

    This model is used in the database.

    ``None`` values mean to use the default config for the frontend,
    and the corresponding config has not been backed up yet.
    """
    account_id: PyObjectId = Field(..., description="Account ID (ObjectID) who has this config.")
    slot_map: dict | None = Field(
        ...,
        description="Slot mapping to Px data identifier. "
                    "Px data identifier should be in the format of `<Security>@<PeriodMin>`."
    )
    layout_config: dict | None = Field(..., description="Layout config of slots.")
    layout_type: LayoutType | None = Field(..., description="Layout type.")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
