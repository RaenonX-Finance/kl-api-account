from typing import TYPE_CHECKING

from fastapi import HTTPException, status

if TYPE_CHECKING:
    from kl_api_account.db import Permission


def generate_unauthorized_exception(message: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=message,
        headers={"WWW-Authenticate": "Bearer"},
    )


def generate_insufficient_permission_exception(permissions: list["Permission"]) -> HTTPException:
    return generate_unauthorized_exception(f"Insufficient permission. Permissions needed: {', '.join(permissions)}")


def generate_blocked_exception() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Account blocked"
    )


def generate_bad_request_exception(message: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=message,
        headers={"WWW-Authenticate": "Bearer"},
    )
