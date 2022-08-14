from fastapi import APIRouter, Depends

from .db_control import get_account_list, update_account_blocked, update_account_expiry, update_account_permission
from .model import AccountData

admin_router = APIRouter(prefix="/admin")


@admin_router.get(
    "/accounts",
    description="Get a list of accounts.",
    response_model=list[AccountData],
)
async def get_accounts(accounts: list[AccountData] = Depends(get_account_list)) -> list[AccountData]:
    return accounts


@admin_router.post(
    "/update-expiry",
    description="Update the membership expiry of an account.",
    response_model=AccountData,
)
async def update_expiry(updated_account: AccountData = Depends(update_account_expiry)) -> AccountData:
    return updated_account


@admin_router.post(
    "/update-blocked",
    description="Update the blocking status of an account.",
    response_model=AccountData,
)
async def update_blocked(updated_account: AccountData = Depends(update_account_blocked)) -> AccountData:
    return updated_account


@admin_router.post(
    "/update-permissions",
    description="Update permissions of an account.",
    response_model=AccountData,
)
async def update_permissions(updated_account: AccountData = Depends(update_account_permission)) -> AccountData:
    return updated_account
