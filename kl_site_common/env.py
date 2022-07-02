import os

from dotenv import load_dotenv
from jose.constants import Algorithms

load_dotenv()

FASTAPI_AUTH_SECRET = os.environ.get("FASTAPI_AUTH_SECRET")
if not FASTAPI_AUTH_SECRET:
    raise ValueError(
        "Set `FASTAPI_AUTH_SECRET` as the FastAPI authentication secret. "
        "Check README for more details."
    )

FAST_API_AUTH_ALGORITHM = os.environ.get("FAST_API_AUTH_ALGORITHM", Algorithms.HS256)

FAST_API_AUTH_TOKEN_EXPIRY_MINS = os.environ.get("FAST_API_AUTH_TOKEN_EXPIRY_MINS", 15)

MONGO_URL = os.environ.get("MONGO_URL")
if not MONGO_URL:
    raise ValueError("Set `MONGO_URL` as the MongoDB connection string. Check README for more details.")
