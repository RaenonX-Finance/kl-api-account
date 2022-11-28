from pydantic import BaseModel, Field


class WebRTCSDPModel(BaseModel):
    sdp: str = Field(...)
    type: str = Field(...)
