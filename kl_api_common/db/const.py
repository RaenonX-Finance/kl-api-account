from pymongo import MongoClient

from kl_api_common.env import MONGO_URL

mongo_client = MongoClient(MONGO_URL, tz_aware=True)
