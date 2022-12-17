from .general import register_handlers_general
from .px import register_handlers_px


def register_handlers():
    register_handlers_general()
    register_handlers_px()
