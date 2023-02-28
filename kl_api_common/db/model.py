from typing import MutableMapping

from bson import ObjectId


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: str | bytes):
        if not ObjectId.is_valid(v):
            raise ValueError(f"Invalid object ID: {v}")

        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema: MutableMapping):
        field_schema.update(type="string")
