from pymongo import MongoClient

from kl_site_common.env import MONGO_URL

mongo_client = MongoClient(MONGO_URL)
