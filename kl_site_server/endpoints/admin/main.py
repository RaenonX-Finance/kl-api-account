from fastapi import APIRouter, Depends

from .db_control import get_account_list, update_account_expiry
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
)
async def update_expiry(updated_account: AccountData = Depends(update_account_expiry)) -> AccountData:
    return updated_account
