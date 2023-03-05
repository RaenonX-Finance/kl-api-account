from fastapi import APIRouter

ping_router = APIRouter(prefix="/ping")


@ping_router.get(
    "/",
    description="Ping server.",
    response_model=str,
)
async def ping() -> str:
    return "pong"
