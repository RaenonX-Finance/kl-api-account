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


class FastApiSioJSONSerializer:
    @staticmethod
    def dumps(*args, **kwargs):
        return json.dumps(*args, **kwargs, cls=JSONEncoder)

    @staticmethod
    def loads(*args, **kwargs):
        return json.loads(*args, **kwargs)
