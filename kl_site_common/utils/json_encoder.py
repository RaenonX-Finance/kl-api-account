import json

from bson import ObjectId
from pydantic import BaseModel


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)

        if isinstance(o, BaseModel):
            return o.dict()

        return json.JSONEncoder.default(self, o)
