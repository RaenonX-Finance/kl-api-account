import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_socketio import SocketManager

from kl_site_common.env import DEVELOPMENT_MODE

fast_api = FastAPI(
    title="KL Site backend API",
    version="0.5.0",
    # Disable docs if not in dev mode
    openapi_url="/openapi.json" if DEVELOPMENT_MODE else None,
)
# Set `cors_allowed_origins` to `None` and let `CORSMiddleware` handle CORS things
# > Calling ``SocketManager`` patches `fast_api` with attribute `sio`
SocketManager(app=fast_api, cors_allowed_origins=[])
fast_api_socket: socketio.AsyncServer = fast_api.sio


fast_api.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.kl-law.net",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
