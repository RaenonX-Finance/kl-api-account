from fastapi import FastAPI
from fastapi_socketio import SocketManager

from .endpoints import auth_router

fast_api = FastAPI(title="KL Site backend API", version="0.3.0")
fast_api_socket = SocketManager(app=fast_api)

fast_api.include_router(auth_router)
