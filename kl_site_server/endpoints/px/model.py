from abc import ABC

from pydantic import BaseModel, Field


class PxRequestParams(BaseModel, ABC):
    """Base model of px data request params."""
    token: str = Field(..., description="Account access token.")


class PxInitParams(PxRequestParams):
    """Px data initialization parameters."""
    identifiers: list[str] = Field(..., description="Identifiers to request data.")


class RequestPxMessageSingle(BaseModel):
    """Single px data request."""
    identifier: str = Field(..., description="Px data identifier.")
    offset: int | None = Field(None, description="Count of bars to offset on the returning px data.")
    limit: int | None = Field(None, description="Maximum count of the bars to get.")


class RequestPxParams(PxRequestParams):
    """Px data request parameters."""
    requests: list[RequestPxMessageSingle] = Field(..., description="Requests of individual px data request.")
