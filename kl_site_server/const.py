from fastapi import FastAPI
from fastapi_socketio import SocketManager

fast_api = FastAPI()
fast_api_socket = SocketManager(app=fast_api)
