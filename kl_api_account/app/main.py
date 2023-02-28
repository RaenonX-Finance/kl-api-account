from datetime import datetime, timedelta

from .routes import register_api_routes
from .socket import register_handlers

latest_date: datetime = datetime.utcnow() + timedelta(hours=1)


def start_server_app():
    register_handlers()
    register_api_routes()
